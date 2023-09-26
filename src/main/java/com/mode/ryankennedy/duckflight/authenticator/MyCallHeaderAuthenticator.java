package com.mode.ryankennedy.duckflight.authenticator;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

public class MyCallHeaderAuthenticator implements CallHeaderAuthenticator {
    @Override
    @WithSpan
    public AuthResult authenticate(CallHeaders incomingHeaders) {
        System.out.printf("HEADERS%n=======%n");
        incomingHeaders.keys().forEach(key ->
                incomingHeaders.getAll(key).forEach(value -> System.out.printf("    %s -> %s%n", key, value)));
        System.out.println();

        if (incomingHeaders.containsKey("username")) {
            var username = incomingHeaders.get("username");
            System.out.printf("username = %s%n", username);
            return () -> username;
        } else if (incomingHeaders.containsKey("authorization")) {
            var authorization = incomingHeaders.get("authorization");
            System.out.printf("authorization = %s%n", authorization);
            return () -> authorization;
        }
        throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }
}
