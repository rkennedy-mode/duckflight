# duckflight

A simple [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) server & proxy to allow DuckDB 
to be queried remotely using the [Arrow Flight SQL JDBC Driver](https://arrow.apache.org/docs/java/flight_sql_jdbc_driver.html).

Note that the proxying is a rough proof-of-concept.

To run
* Start the server by passing "server" as the command line argument.
* Start the proxy by passing "proxy" as the argument.
* Run the client by passing "client" as the argument.

This project uses Java 17.
