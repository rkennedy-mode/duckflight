package com.mode.ryankennedy.duckflight;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.apache.arrow.flight.Location;

import java.sql.*;

/**
 * A simple program, which starts an Arrow Flight SQL (backed by an empty, in-memory DuckDB)
 * and connects to it using the Arrow Flight SQL JDBC driver to execute a simple SELECT query
 * of the information schema to verify that we can return rows.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        // Start up the server
        try (var server = Server.started("jdbc:duckdb:")) {
            // Issue a SQL query to the Arrow Flight server
            query(server.getLocation());
        }
    }

    @WithSpan
    private static void query(Location serverLocation) {
        // Generate a JDBC connection using the Arrow Flight SQL JDBC driver
        try (var connection = getConnection(serverLocation)) {
            try (var statement = connection.createStatement();
                 // Attempt to query the information schema
                 var results = executeQuery(statement)) {
                consumeResults(results);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @WithSpan
    private static Connection getConnection(Location serverLocation) throws SQLException {
        // Connect to the Arrow Flight SQL server using token authentication and no encryption
        return DriverManager.getConnection("jdbc:arrow-flight-sql://%s:%d/?%s".formatted(
                serverLocation.getUri().getHost(),
                serverLocation.getUri().getPort(),
                "token=token_1&useEncryption=false"));
    }

    @WithSpan
    private static void consumeResults(ResultSet results) throws SQLException {
        // Output the column names and values for each row
        var metaData = results.getMetaData();
        while (results.next()) {
            System.out.printf("Row #%d%n", results.getRow());
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                System.out.printf("    %s: %s%n", metaData.getColumnName(i), results.getString(i));
            }
        }
    }

    @WithSpan
    private static ResultSet executeQuery(Statement statement) throws SQLException {
        // Query the information schema's `schemata` table, which is one of the few with rows in
        // it after connecting to a fresh DuckDB instance
        return statement.executeQuery("select * from information_schema.schemata");
    }
}
