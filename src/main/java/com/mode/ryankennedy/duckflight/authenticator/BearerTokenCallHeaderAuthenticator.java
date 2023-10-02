package com.mode.ryankennedy.duckflight.authenticator;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

public class BearerTokenCallHeaderAuthenticator implements CallHeaderAuthenticator {
    @Override
    @WithSpan
    public AuthResult authenticate(CallHeaders incomingHeaders) {
        var bearerToken = AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
        if (bearerToken != null) {
            // TODO: Utilize a cache to minimize calls to webapp
            // TODO: Make the call to webapp to validate
            return () -> bearerToken;
        }
        throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }
}
