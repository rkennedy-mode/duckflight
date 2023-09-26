package com.mode.ryankennedy.duckflight.producer;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
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
import java.util.UUID;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.jdbcToArrowSchema;

public class MyFlightSqlProducer implements FlightSqlProducer {
    public static final Calendar CALENDAR = JdbcToArrowUtils.getUtcCalendar();
    private final Location serverLocation;
    private final Connection connection;

    public MyFlightSqlProducer(Location serverLocation) {
        this.serverLocation = serverLocation;
        try {
            connection = DriverManager.getConnection("jdbc:duckdb:");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @WithSpan
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        System.out.println("MyFlightSqlProducer.createPreparedStatement: sql = " + request.getQuery());

        var preparedStatementHandle = ByteString.copyFromUtf8(UUID.randomUUID().toString());

        try (var preparedStatement = connection.prepareStatement(request.getQuery());
             var resultSet = preparedStatement.executeQuery()) {

            var parameterSchema = jdbcToArrowSchema(preparedStatement.getParameterMetaData(), CALENDAR);
            var datasetSchema = jdbcToArrowSchema(resultSet.getMetaData(), CALENDAR);

            var result = FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                    .setDatasetSchema(ByteString.copyFrom(serializeMetadata(datasetSchema)))
                    .setParameterSchema(ByteString.copyFrom(serializeMetadata(parameterSchema)))
                    .setPreparedStatementHandle(preparedStatementHandle)
                    .build();

            listener.onNext(new Result(Any.pack(result).toByteArray()));
            listener.onCompleted();
        } catch (SQLException e) {
            listener.onError(CallStatus.INTERNAL
                    .withCause(e)
                    .withDescription(e.getMessage())
                    .toRuntimeException());
        }
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
        listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
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
        return null;

//        var preparedStatementHandle = command.getPreparedStatementHandle();
//        var statementContext = preparedStatementLoadingCache.getIfPresent(preparedStatementHandle);
//        try {
//            assert statementContext != null;
//            PreparedStatement statement = statementContext.getStatement();
//
//            ResultSetMetaData metaData = statement.getMetaData();
//            return getFlightInfoForSchema(command, descriptor,
//                    jdbcToArrowSchema(metaData, CALENDAR));
//        } catch (final SQLException e) {
//            throw CallStatus.INTERNAL
//                    .withCause(e)
//                    .withDescription(e.getMessage())
//                    .toRuntimeException();
//        }
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
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamPreparedStatement");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
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
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
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
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
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
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
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
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
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
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
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
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
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
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
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
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamImportedKeys");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context, ServerStreamListener listener) {
        System.out.println("MyFlightSqlProducer.getStreamCrossReference");
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
    }

    @Override
    @WithSpan
    public void close() throws Exception {
        System.out.println("MyFlightSqlProducer.close");
        connection.close();
    }

    @Override
    @WithSpan
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        System.out.println("MyFlightSqlProducer.listFlights");
        listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
    }
}
