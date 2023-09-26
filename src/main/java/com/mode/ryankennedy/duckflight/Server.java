package com.mode.ryankennedy.duckflight;

import com.mode.ryankennedy.duckflight.authenticator.MyCallHeaderAuthenticator;
import com.mode.ryankennedy.duckflight.producer.MyFlightSqlProducer;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

import java.sql.DriverManager;
import java.sql.SQLException;

public class Server {
    private static final String TOKEN_QUERY_STRING = "token=token_1&useEncryption=false";

    @WithSpan
    public static void main(String[] args) {
        // Configure the Arrow Flight (gRPC) server to bind to some available port on 0.0.0.0.
        var location = Location.forGrpcInsecure("0.0.0.0", 0);
        try (var producer = new MyFlightSqlProducer(location);
             var flightServer = FlightServer.builder()
                     .allocator(new RootAllocator())
                     .location(location)
                     .headerAuthenticator(new MyCallHeaderAuthenticator())
                     .producer(producer)
                     .build()) {
            // Start the server
            flightServer.start();

            // Note the bound host+port for anyone listening.
            System.out.printf("Flight Server listening: %s%n", flightServer.getLocation().getUri());

            // Issue a SQL query to the Arrow Flight server
            query(flightServer.getLocation());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @WithSpan
    private static void query(Location serverLocation) {
        // Generate a JDBC connection using the Arrow Flight SQL JDBC driver
        try (var connection = DriverManager.getConnection("jdbc:arrow-flight-sql://%s:%d/?%s".formatted(
                serverLocation.getUri().getHost(),
                serverLocation.getUri().getPort(),
                TOKEN_QUERY_STRING))) {
            try (var statement = connection.createStatement();
                 // Attempt to query the information schema
                 var results = statement.executeQuery("SELECT * FROM information_schema.tables")) {
                while (results.next()) {
                    // Iterate through the results
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
