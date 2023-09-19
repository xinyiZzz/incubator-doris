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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Dremio authentication specialized CredentialValidator implementation.
 */
public class FlightServerCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {
    private static final Logger LOG = LogManager.getLogger(FlightServerCredentialValidator.class);

    /**
     * Authenticates against Dremio with the provided username and password.
     *
     * @param username Dremio username.
     * @param password Dremio user password.
     * @return AuthResult with username as the peer identity.
     */
    @Override
    public AuthResult validate(String username, String password) {
        // String remoteIp = context.getMysqlChannel().getRemoteIp();
        String remoteIp = "0.0.0.0";
        List<UserIdentity> currentUserIdentity = Lists.newArrayList();

        try {
            Env.getCurrentEnv().getAuth().checkPlainPassword(username, remoteIp, password, currentUserIdentity);
        } catch (AuthenticationException e) {
            LOG.error("Unable to authenticate user {}", username, e);
            final String errorMessage = "Unable to authenticate user " + username + ", exception: " + e.getMessage();
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(errorMessage).toRuntimeException();
        }
        Preconditions.checkState(currentUserIdentity.size() == 1);
        // context.setCurrentUserIdentity(currentUserIdentity.get(0));
        // context.setRemoteIP(remoteIp);
        org.apache.doris.service.arrowflight.AuthResult authResult = org.apache.doris.service.arrowflight.AuthResult.of(
                username);
        return authResult::getUserName;
    }
}
