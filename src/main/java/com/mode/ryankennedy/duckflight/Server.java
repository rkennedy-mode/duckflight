package com.mode.ryankennedy.duckflight;

import com.mode.ryankennedy.duckflight.authenticator.BearerTokenCallHeaderAuthenticator;
import com.mode.ryankennedy.duckflight.producer.JdbcFlightSqlProducer;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

public class Server implements AutoCloseable {
    private final String connectionString;
    private RootAllocator rootAllocator;
    private FlightServer flightServer;
    private JdbcFlightSqlProducer producer;

    public static Server started(String connectionString) {
        var server = new Server(connectionString);
        server.start();
        return server;
    }

    private Server(String connectionString) {
        this.connectionString = connectionString;
    }

    @WithSpan
    private void start() {
        rootAllocator = new RootAllocator();

        // Configure the Arrow Flight (gRPC) server to bind to some available port on 0.0.0.0.
        var location = Location.forGrpcInsecure("0.0.0.0", 0);

        try {
            producer = new JdbcFlightSqlProducer(location, connectionString, rootAllocator);
            flightServer = FlightServer.builder()
                    .allocator(rootAllocator)
                    .location(location)
                    .headerAuthenticator(new BearerTokenCallHeaderAuthenticator())
                    .producer(producer)
                    .build();

            // Start the server
            flightServer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Location getLocation() {
        return flightServer.getLocation();
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    private void stop() throws Exception {
        flightServer.close();
        producer.close();
        rootAllocator.close();
    }
}
