package com.mode.ryankennedy.duckflight;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerTest {
    private static final String TOKEN_QUERY_STRING = "token=token_1&useEncryption=false";
    private static final String TEST_DATA_DB_FILENAME = "test_data.db";

    @TempDir
    private Path temporaryDirectory;
    private Server server;
    private String connectionString;

    @BeforeEach
    void setUp() throws Exception {
        var testDbConnectionString = populateDatabase();

        server = Server.started(testDbConnectionString);
        connectionString = "jdbc:arrow-flight-sql://%s:%d/?%s".formatted(
                server.getLocation().getUri().getHost(),
                server.getLocation().getUri().getPort(),
                TOKEN_QUERY_STRING);
    }

    @AfterEach
    void tearDown() throws Exception {
        server.close();
    }

    @Test
    void testConnect() throws Exception {
        try (var connection = getConnection()) {
            // Execute isValid to make an effort to actually check that the connection is
            // established and working (according to whatever criteria the driver uses).
            connection.isValid(Math.toIntExact(Duration.ofSeconds(3).toSeconds()));
        }
    }

    @Test
    void testDatabaseMetadata() throws Exception {
        try (var connection = getConnection();
             var schemas = connection.getMetaData().getSchemas("memory", null)) {
            var foundSchema = false;
            while (schemas.next()) {
                foundSchema = foundSchema || schemas.getString("TABLE_SCHEM").equalsIgnoreCase("test_schema");
            }
            assertTrue(foundSchema, "Schema 'test_schema' was not found in the DatabaseMetaData");
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(connectionString);
    }

    private String populateDatabase() throws Exception {
        var testDbPath = temporaryDirectory.resolve(TEST_DATA_DB_FILENAME);
        var testDbConnectionString = "jdbc:duckdb:%s".formatted(testDbPath.toAbsolutePath());
        try (var connection = DriverManager.getConnection(testDbConnectionString);
             var statement = connection.createStatement()) {
            statement.executeUpdate("""
                    CREATE SCHEMA test_schema;
                    CREATE TABLE test_schema.test_data(id INTEGER, greeting VARCHAR, locale VARCHAR, is_default BOOLEAN);
                    INSERT INTO test_schema.test_data VALUES (1, 'Hello, World!', 'english', true);
                    INSERT INTO test_schema.test_data VALUES (2, 'Bonjour le monde!', 'french', false);
                    INSERT INTO test_schema.test_data VALUES (3, '¡Buen mundo!', 'spanish', false);
                    """);
            return testDbConnectionString;
        }
    }
}