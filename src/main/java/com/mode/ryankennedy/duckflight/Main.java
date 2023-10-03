package com.mode.ryankennedy.duckflight;

import org.apache.arrow.util.AutoCloseables;

/**
 * A simple program, which starts an Arrow Flight SQL (backed by an empty, in-memory DuckDB)
 * and connects to it using the Arrow Flight SQL JDBC driver to execute a simple SELECT query
 * of the information schema to verify that we can return rows.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        // Start up the server
        if (args.length == 0) {
            throw new IllegalArgumentException("Must provide run type: 'server', 'proxy' or 'client'");
        }

        switch (args[0]) {
            case "client" -> {
                System.out.println("Starting client");
                var client = new Client(Proxy.LOCATION);
                client.query();
                System.out.println("Client finished");
            }
            case "proxy" -> {
                System.out.println("Starting proxy, blocking forever");
                var proxy = new Proxy(Server.HOST, Server.PORT);
                System.out.println("Proxy finished");
            }
            case "server" -> {
                System.out.println("Starting server, blocking forever");
                AutoCloseables.close(Server.started("jdbc:duckdb:"));
                System.out.println("Server finished");
            }
            default -> throw new IllegalArgumentException("Unsupported type: " + args[0]);
        }
    }
}
