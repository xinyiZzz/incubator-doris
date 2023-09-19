// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// This file is copied from
// https://github.com/dremio/dremio-oss/blob/master/services/arrow-flight/src/main/java/com/dremio/service/flight/ServerCookieMiddleware.java
// and modified by Doris

package org.apache.doris.service.arrowflight;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlightServerBearerTokenAuthenticator implements CallHeaderAuthenticator {
    private static final Logger LOG = LogManager.getLogger(FlightServerBearerTokenAuthenticator.class);

    private final CallHeaderAuthenticator initialAuthenticator;

    public FlightServerBearerTokenAuthenticator() {
        this.initialAuthenticator = new BasicCallHeaderAuthenticator(new FlightServerCredentialValidator());
    }

    @Override
    public AuthResult authenticate(CallHeaders incomingHeaders) {
        final String bearerToken = AuthUtilities.getValueFromAuthHeader(incomingHeaders,
                Auth2Constants.BEARER_PREFIX);

        if (bearerToken != null) {
            return validateBearer(bearerToken);
        } else {
            final AuthResult result = initialAuthenticator.authenticate(incomingHeaders);
            return getAuthResultWithBearerToken(result, incomingHeaders);
        }
    }

    /**
     * Validates provided token.
     *
     * @param token the token to validate.
     * @return an AuthResult with the bearer token and peer identity.
     */
    @VisibleForTesting
    AuthResult validateBearer(String token) {
        try {
            // tokenManagerProvider.get().validateToken(token);
            return createAuthResultWithBearerToken(token);
        } catch (IllegalArgumentException e) {
            LOG.error("Bearer token validation failed.", e);
            throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
    }

    /**
     * Generates a bearer token, parses client properties from incoming headers, then creates a
     * UserSession associated with the generated token and client properties.
     *
     * @param authResult the AuthResult from initial authentication, with peer identity captured.
     * @param incomingHeaders the CallHeaders to parse client properties from.
     * @return an an AuthResult with the bearer token and peer identity.
     */
    @VisibleForTesting
    AuthResult getAuthResultWithBearerToken(AuthResult authResult, CallHeaders incomingHeaders) {
        final String username = authResult.getPeerIdentity();
        // final String token = DremioFlightAuthUtils.createUserSessionWithTokenAndProperties(
        //         tokenManagerProvider,
        //         username);

        return createAuthResultWithBearerToken(username);
    }

    /**
     * Helper method to create an AuthResult.
     *
     * @param token the token to create a UserSession for.
     * @return a new AuthResult with functionality to add given bearer token to the outgoing header.
     */
    private AuthResult createAuthResultWithBearerToken(String token) {
        return new AuthResult() {
            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                        Auth2Constants.BEARER_PREFIX + token);
            }

            @Override
            public String getPeerIdentity() {
                return token;
            }
        };
    }
}
