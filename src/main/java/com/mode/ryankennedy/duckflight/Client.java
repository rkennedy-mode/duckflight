package com.mode.ryankennedy.duckflight;

import org.apache.arrow.flight.Location;

import java.sql.*;

public class Client {

    private final Location location;

    public Client(Location location) {
        this.location = location;
    }

    public void query() {
        // Generate a JDBC connection using the Arrow Flight SQL JDBC driver
        try (var connection = getConnection();
             var statement = connection.createStatement();
             // Attempt to query the information schema
             var results = executeQuery(statement)) {
                consumeResults(results);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection() throws SQLException {
        // Connect to the Arrow Flight SQL server using token authentication and no encryption
        return DriverManager.getConnection("jdbc:arrow-flight-sql://%s:%d/?%s".formatted(
                location.getUri().getHost(),
                location.getUri().getPort(),
                "token=token_1&useEncryption=false"));
    }

    private void consumeResults(ResultSet results) throws SQLException {
        // Output the column names and values for each row
        var metaData = results.getMetaData();
        while (results.next()) {
            System.out.printf("Row #%d%n", results.getRow());
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                System.out.printf("    %s: %s%n", metaData.getColumnName(i), results.getString(i));
            }
        }
    }

    private ResultSet executeQuery(Statement statement) throws SQLException {
        // Query the information schema's `schemata` table, which is one of the few with rows in
        // it after connecting to a fresh DuckDB instance
        return statement.executeQuery("select * from information_schema.schemata");
    }
}
