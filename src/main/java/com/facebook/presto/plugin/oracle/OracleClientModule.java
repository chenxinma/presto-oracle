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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import oracle.jdbc.OracleDriver;

import java.sql.SQLException;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Guice implementation to create the correct DI and binds
 * 
 * @author Marcelo Paes Rech
 *
 */
public class OracleClientModule extends AbstractConfigurationAwareModule {
	@Override
	protected void setup(Binder binder)
	{
		binder.bind(JdbcClient.class).to(OracleClient.class).in(Scopes.SINGLETON);
		ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
		configBinder(binder).bindConfig(OracleConfig.class);
	}

	private static void ensureCatalogIsEmpty(String connectionUrl)
	{
		try {
			OracleDriver driver = new OracleDriver();
			boolean bAccept = driver.acceptsURL(connectionUrl);
			checkArgument(bAccept, "Invalid JDBC URL for Oracle connector");
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
