package com.mode.ryankennedy.duckflight;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.flight.Location;

import java.io.IOException;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class Proxy {
    public static final Location LOCATION = Location.forGrpcInsecure("0.0.0.0", 3001);

    private static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);
    private static final Context.Key<String> WORKSPACE_TOKEN_KEY = Context.key("WorkspaceToken");

    private final FlightServiceGrpc.FlightServiceBlockingStub client;

    public Proxy(String serverHost, int serverPort) throws InterruptedException, IOException {
        var channel = Grpc.newChannelBuilder(serverHost + ":" + serverPort, InsecureChannelCredentials.create())
                .build();
        this.client = FlightServiceGrpc.newBlockingStub(channel)
                .withInterceptors(new ClientWorkspaceTokenInterceptor());

        var server = Grpc.newServerBuilderForPort(3001, InsecureServerCredentials.create())
                .addService(new GrpcServer())
                .intercept(new ServerWorkspaceTokenInterceptor())
                .build()
                .start();
        server.awaitTermination();
    }

    private class GrpcServer extends FlightServiceGrpc.FlightServiceImplBase {

        @Override
        public void doAction(Flight.Action request, StreamObserver<Flight.Result> responseObserver) {
            var token = WORKSPACE_TOKEN_KEY.get();
            System.out.println("Proxy.GrpcServer.doAction (Token: " + token + ") - " + request.toString());
            try {
                var results = client.doAction(request);
                while (results.hasNext()) {
                    responseObserver.onNext(results.next());
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                System.out.println("  Exception - " + e.getMessage());
                responseObserver.onError(e);
            }
        }

        @Override
        public void doGet(Flight.Ticket request, StreamObserver<Flight.FlightData> responseObserver) {
            System.out.println("Proxy.GrpcServer.doGet - " + request);
            try {
                var flightData = client.doGet(request);
                while (flightData.hasNext()) {
                    responseObserver.onNext(flightData.next());
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                System.out.println("  Exception - " + e.getMessage());
                responseObserver.onError(e);
            }
        }

        @Override
        public void getFlightInfo(Flight.FlightDescriptor request, StreamObserver<Flight.FlightInfo> responseObserver) {
            System.out.println("Proxy.GrpcServer.getFlightInfo - " + request);
            try {
                responseObserver.onNext(client.getFlightInfo(request));
                responseObserver.onCompleted();
            } catch (Exception e) {
                System.out.println("  Exception - " + e.getMessage());
                responseObserver.onError(e);
            }
        }

        @Override
        public StreamObserver<Flight.HandshakeRequest> handshake(StreamObserver<Flight.HandshakeResponse> responseObserver) {
            System.out.println("Proxy.GrpcServer.handshake");
            // TODO: no idea if this is correct
            var response = Flight.HandshakeResponse.getDefaultInstance();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return null;
        }

        @Override
        public void listFlights(Flight.Criteria request, StreamObserver<Flight.FlightInfo> responseObserver) {
            System.out.println("Proxy.GrpcServer.listFlights");
            try {
                var flights = client.listFlights(request);
                while (flights.hasNext()) {
                    responseObserver.onNext(flights.next());
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                System.out.println("  Exception - " + e.getMessage());
                responseObserver.onError(e);
            }
        }
    }

    private static class ServerWorkspaceTokenInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
            var token = headers.get(AUTHORIZATION_METADATA_KEY);
            System.out.println("Server Intercepted - Token: " + token);
            var ctx = Context.current()
                    .withValue(WORKSPACE_TOKEN_KEY, token.replace("Bearer ", ""));
            return Contexts.interceptCall(ctx, call, headers, next);
        }
    }

    private static class ClientWorkspaceTokenInterceptor implements ClientInterceptor {

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            System.out.println("Client Intercepted");
            return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    headers.put(AUTHORIZATION_METADATA_KEY, "Bearer " + WORKSPACE_TOKEN_KEY.get());
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
