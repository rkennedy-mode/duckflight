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

    // Configure the Arrow Flight (gRPC) server to bind to port 3000 on 0.0.0.0.
    public static String HOST = "0.0.0.0";
    public static int PORT = 3000;
    public static Location LOCATION = Location.forGrpcInsecure(HOST, PORT);

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

        try {
            producer = new JdbcFlightSqlProducer(LOCATION, connectionString, rootAllocator);
            flightServer = FlightServer.builder()
                    .allocator(rootAllocator)
                    .location(LOCATION)
                    .headerAuthenticator(new BearerTokenCallHeaderAuthenticator())
                    .producer(producer)
                    .build();

            // Start the server
            var server = flightServer.start();
            server.awaitTermination();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
