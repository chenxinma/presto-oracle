/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Decimals;
import com.google.common.collect.ImmutableSet;
import oracle.jdbc.OracleDriver;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import java.sql.*;
import java.util.*;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.decimalReadMapping;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static java.lang.Math.max;

/**
 * Implementation of OracleClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 * 
 * @author Marcelo Paes Rech
 *
 */
public class OracleClient extends BaseJdbcClient {

	private static final Logger log = Logger.getLogger(OracleClient.class);

	public static List<String> ignoreSchemas = Arrays.asList("anonymous", "olapsys", "sys", "system", "xdb", "xs$null");

	private static ConnectionFactory connectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig) {
		Properties connectionProperties = basicConnectionProperties(config);
		if (oracleConfig.getConnectionTimeout() != null) {
			connectionProperties.setProperty("oracle.net.CONNECT_TIMEOUT", String.valueOf(oracleConfig.getConnectionTimeout().toMillis()));
		}
		return new DriverConnectionFactory(new OracleDriver(), config.getConnectionUrl(), connectionProperties);
	}

	@Inject
	public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config,
			OracleConfig oracleConfig) throws SQLException {
		super(connectorId, config, "", connectionFactory(config, oracleConfig));
	}

	@Override
	protected Collection<String> listSchemas(Connection connection) {
		try {
			ResultSet resultSet = connection.getMetaData().getSchemas();
			Throwable t = null;
			try {
				ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
				while (resultSet.next()) {
					String schemaName = resultSet.getString(1).toLowerCase();
					if (!ignoreSchemas.contains(schemaName)) {
						schemaNames.add(schemaName);
					}
				}
				return schemaNames.build();
			} catch (Throwable e) {
				t = e;
				throw e;
			} finally {
				if (resultSet != null) {
					if (t != null) {
						try {
							resultSet.close();
						} catch (Throwable t2) {
							t.addSuppressed(t2);
						}
					} else {
						resultSet.close();
					}
				}

			}
		} catch (SQLException ex) {
			throw new PrestoException(JdbcErrorCode.JDBC_ERROR, ex);
		}
	}

	@Override
	protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
			throws SQLException {
		// Here we put TABLE and SYNONYM when the table schema is another user schema
		DatabaseMetaData metadata = connection.getMetaData();
		Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
		return metadata.getTables(
				schemaName.orElse(null),
				null,
				escapeNamePattern(tableName, escape).orElse(null),
				new String[] {"TABLE", "VIEW", "SYNONYM"});
	}

	@Override
	public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle type) {
		Optional<ReadMapping> tp = super.toPrestoType(session, type);
		if (!tp.isPresent()) {
			int columnSize = type.getColumnSize();
			switch (type.getJdbcType()) {
				case Types.NUMERIC:
				case Types.DECIMAL:
					int decimalDigits = type.getDecimalDigits();
					if (decimalDigits == -127) {
						decimalDigits = 0;
						columnSize = 22;
					}
					int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
					if (precision > Decimals.MAX_PRECISION) {
						return Optional.empty();
					}
					return Optional.of(decimalReadMapping(createDecimalType(precision, max(decimalDigits, 0))));
			}
		} else {
			return tp;
		}
		return Optional.empty();
	}
}
