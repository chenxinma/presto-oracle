package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import oracle.jdbc.OracleDriver;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

import static org.junit.jupiter.api.Assertions.assertTrue;

class OracleClientModuleTest {
    private static final Logger log = Logger.getLogger(OracleClientModuleTest.class);
    private static final ConnectorSession session = testSessionBuilder().build().toConnectorSession();

    public static final String CONNECTOR_ID = "test";

    @Test
    public void connectUrlTest() throws SQLException {
        String connectionUrl = "jdbc:oracle:thin:@//10.18.20.180:1521/MUDATA";
        OracleDriver driver = new OracleDriver();
        assertTrue(driver.acceptsURL(connectionUrl));
    }

    @Test
    public void listSchemasTest() throws SQLException, ClassNotFoundException {
        String connectionUrl = "jdbc:oracle:thin:@//10.18.20.180:1521/MUDATA";
        Class.forName("oracle.jdbc.OracleDriver");
        Connection connection =
                DriverManager.getConnection(connectionUrl, "md_flight", "MD_FLIGHT_2018");
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase();
                log.info("Listing schemas: " + schemaName);
                schemaNames.add(schemaName);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        log.info("end.");
    }

    @Test
    public void getColumnsTest() throws SQLException, ClassNotFoundException {
        String connectionUrl = "jdbc:oracle:thin:@//10.18.20.180:1521/MUDATA";
        Class.forName("oracle.jdbc.OracleDriver");
        JdbcConnectorId connectorId = new JdbcConnectorId(CONNECTOR_ID);
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setConnectionUrl(connectionUrl);
        config.setConnectionUser("MD_BOOKING");
        config.setConnectionPassword("MD_BOOKING_2019");
        OracleConfig oracleConfig = new OracleConfig();
        OracleClient client = new OracleClient(connectorId, config, oracleConfig);

        List<JdbcColumnHandle> columns = client.getColumns(session,
                client.getTableHandle(JdbcIdentity.from(session),
                        new SchemaTableName("md_booking", "t07_pass_load_factor")));
        columns.forEach(c -> log.info(String.format("%s.%s", c.getColumnName(), c.getColumnType().toString())));
        log.info("end.");
    }
}