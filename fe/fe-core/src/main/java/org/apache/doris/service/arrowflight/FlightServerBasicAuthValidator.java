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

import com.google.common.base.Charsets;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Optional;

/**
 * authentication specialized implementation of BasicAuthValidator. Authenticates with provided
 * credentials.
 * TODO
 */
public class FlightServerBasicAuthValidator implements BasicServerAuthHandler.BasicAuthValidator {
    private static final Logger LOG = LogManager.getLogger(FlightServerBasicAuthValidator.class);

    private static final String myToken = "DORIS_READ_WRITE_TOKEN";

    public FlightServerBasicAuthValidator() {
    }

    @Override
    public byte[] getToken(String username, String password) {
        LOG.info("getToken " + username + password);
        return myToken.getBytes(Charsets.UTF_8);
    }

    @Override
    public Optional<String> isValid(byte[] bytes) {
        // final String token = new String(bytes, Charsets.UTF_8);
        LOG.info("isValid " + Arrays.toString(bytes));
        return Optional.of(myToken);
    }
}
