package com.mode.ryankennedy.duckflight.producer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.sql.*;
import java.util.Calendar;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.jdbcToArrowSchema;

/**
 * A JDBC-powered {@link FlightSqlProducer} implementation.
 * <p/>
 * Much of this implementation is lifted from the
 * <a href="https://github.com/apache/arrow/blob/aca1d3eeed3775c2f02e9f5d59d62478267950b1/java/flight/flight-sql/src/test/java/org/apache/arrow/flight/sql/example/FlightSqlExample.java">FlightSqlExample</a>
 * class provided by the Arrow project.
 * <p/>
 * This is not a first class implementation. This is a very rough cut involving many iterations to get
 * to something that would work well enough to handle the work being done in {@link com.mode.ryankennedy.duckflight.Main}.
 */
public class JdbcFlightSqlProducer implements FlightSqlProducer {
    private static final Calendar CALENDAR = JdbcToArrowUtils.getUtcCalendar();

    private final ExecutorService executorService = Executors.newFixedThreadPool(10,
            new ThreadFactoryBuilder().setDaemon(true).build());
    private final Location serverLocation;
    private final RootAllocator allocator;
    private final Connection connection;
    private final Cache<ByteString, StatementContext> preparedStatementLoadingCache;

    @WithSpan
    public JdbcFlightSqlProducer(Location serverLocation, String connectionString, RootAllocator allocator) {
        this.serverLocation = serverLocation;
        this.allocator = allocator;

        try {
            // Connect to the database indicated in the connection string.
            // TODO: Consider switching to a pooled connection of some sort or allowing a subclass of this class to implement connection fetching (e.g. using DuckDB's duplicate() method).
            connection = DriverManager.getConnection(connectionString);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // The prepared statement loading cache is how the asynchronous methods of Arrow Flight SQL find running
        // statements across gRPC invocations
        preparedStatementLoadingCache = CacheBuilder.newBuilder()
                .maximumSize(100)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .removalListener((RemovalNotification<ByteString, StatementContext> notification) -> {
                    try {
                        AutoCloseables.close(notification.getValue());
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                        e.printStackTrace(System.err);
                    }
                })
                .build();
    }

    @Override
    @WithSpan
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        System.out.println("MyFlightSqlProducer.createPreparedStatement: sql = " + request.getQuery());

        executorService.submit(() -> {
            var preparedStatementHandle = ByteString.copyFromUtf8(UUID.randomUUID().toString());

            try {
                var preparedStatement = connection.prepareStatement(request.getQuery());
                var preparedStatementContext = new StatementContext(preparedStatement);
                preparedStatementLoadingCache.put(preparedStatementHandle, preparedStatementContext);

                var parameterSchema = jdbcToArrowSchema(preparedStatement.getParameterMetaData(), CALENDAR);
                var datasetSchema = jdbcToArrowSchema(preparedStatement.getMetaData(), CALENDAR);

                var result = FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                        .setDatasetSchema(ByteString.copyFrom(serializeMetadata(datasetSchema)))
                        .setParameterSchema(ByteString.copyFrom(serializeMetadata(parameterSchema)))
                        .setPreparedStatementHandle(preparedStatementHandle)
                        .build();

                listener.onNext(new Result(Any.pack(result).toByteArray()));
                listener.onCompleted();
            } catch (SQLException e) {
                System.err.println(e.getMessage());
                e.printStackTrace(System.err);
                listener.onError(CallStatus.INTERNAL
                        .withCause(e)
                        .withDescription(e.getMessage())
                        .toRuntimeException());
            }
        });
    }

    private static ByteBuffer serializeMetadata(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);

            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize schema", e);
        }
    }

    @Override
    @WithSpan
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        System.out.println("MyFlightSqlProducer.closePreparedStatement");

        try {
            preparedStatementLoadingCache.invalidate(request.getPreparedStatementHandle());
            listener.onCompleted();
        } catch (Exception e) {
            listener.onError(CallStatus.INTERNAL.withCause(e).toRuntimeException());
        }
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoStatement");
        return null;
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoPreparedStatement");

        var preparedStatementHandle = command.getPreparedStatementHandle();
        var statementContext = preparedStatementLoadingCache.getIfPresent(preparedStatementHandle);
        try {
            assert statementContext != null;
            var statement = statementContext.statement();

            ResultSetMetaData metaData = statement.getMetaData();
            return getFlightInfoForSchema(command, descriptor,
                    jdbcToArrowSchema(metaData, CALENDAR));
        } catch (final SQLException e) {
            throw CallStatus.INTERNAL
                    .withCause(e)
                    .withDescription(e.getMessage())
                    .toRuntimeException();
        }
    }

    private <T extends Message> FlightInfo getFlightInfoForSchema(final T request, final FlightDescriptor descriptor,
                                                                  final Schema schema) {
        final Ticket ticket = new Ticket(Any.pack(request).toByteArray());
        var endpoints = Collections.singletonList(new FlightEndpoint(ticket, serverLocation));

        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    @Override
    @WithSpan
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getSchemaStatement");
        return null;
    }

    @Override
    @WithSpan
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamStatement");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamStatement not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamPreparedStatement");

        final ByteString handle = command.getPreparedStatementHandle();
        StatementContext statementContext = preparedStatementLoadingCache.getIfPresent(handle);
        Objects.requireNonNull(statementContext);
        final PreparedStatement statement = statementContext.statement();
        try (final ResultSet resultSet = statement.executeQuery()) {
            final Schema schema = jdbcToArrowSchema(resultSet.getMetaData(), CALENDAR);
            try (final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)) {
                final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                listener.start(vectorSchemaRoot);

                var iterator = JdbcToArrow.sqlToArrowVectorIterator(resultSet, allocator);
                while (iterator.hasNext()) {
                    final VectorSchemaRoot batch = iterator.next();
                    if (batch.getRowCount() == 0) {
                        break;
                    }
                    final VectorUnloader unloader = new VectorUnloader(batch);
                    loader.load(unloader.getRecordBatch());
                    listener.putNext();
                    vectorSchemaRoot.clear();
                }

                listener.putNext();
            }
        } catch (final SQLException | IOException e) {
            System.out.printf("Failed to getStreamPreparedStatement: <%s>.%n", e.getMessage());
            listener.error(CallStatus.INTERNAL.withDescription("Failed to prepare statement: " + e).toRuntimeException());
        } finally {
            listener.completed();
        }
    }

    @Override
    @WithSpan
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        System.out.println("MyFlightSqlProducer.acceptPutStatement");
        return null;
    }

    @Override
    @WithSpan
    public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        System.out.println("MyFlightSqlProducer.acceptPutPreparedStatementUpdate");
        return null;
    }

    @Override
    @WithSpan
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        System.out.println("MyFlightSqlProducer.acceptPutPreparedStatementQuery");
        return null;
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoSqlInfo");
        return null;
    }

    @Override
    @WithSpan
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamSqlInfo");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamSqlInfo not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoTypeInfo");
        return null;
    }

    @Override
    @WithSpan
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamTypeInfo");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamTypeInfo not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoCatalogs");
        return null;
    }

    @Override
    @WithSpan
    public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamCatalogs");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamCatalogs not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoSchemas");
        return null;
    }

    @Override
    @WithSpan
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamSchemas");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamSchemas not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoTables");
        return null;
    }

    @Override
    @WithSpan
    public void getStreamTables(FlightSql.CommandGetTables command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamTables");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamTables not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoTableTypes");
        return null;
    }

    @Override
    @WithSpan
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamTableTypes");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamTableTypes not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoPrimaryKeys");
        return null;
    }

    @Override
    @WithSpan
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamPrimaryKeys");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamPrimaryKeys not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoExportedKeys");
        return null;
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoImportedKeys");
        return null;
    }

    @Override
    @WithSpan
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request, CallContext context, FlightDescriptor descriptor) {
        System.out.println("MyFlightSqlProducer.getFlightInfoCrossReference");
        return null;
    }

    @Override
    @WithSpan
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamExportedKeys");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamExportedKeys not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamImportedKeys");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamImportedKeys not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamCrossReference");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("getStreamCrossReference not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public void close() throws Exception {
        System.out.println("MyFlightSqlProducer.close");
        preparedStatementLoadingCache.cleanUp();
        connection.close();
    }

    @Override
    @WithSpan
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        System.out.println("MyFlightSqlProducer.listFlights");
        listener.onError(CallStatus.UNIMPLEMENTED.withDescription("listFlights not implemented.").toRuntimeException());
    }
}
