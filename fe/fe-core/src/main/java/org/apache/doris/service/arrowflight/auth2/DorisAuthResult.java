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

package org.apache.doris.service.arrowflight.auth2;

import org.apache.doris.analysis.UserIdentity;

import org.immutables.value.Value;

/**
 * Result of Authentication.
 */
@Value.Immutable
public interface DorisAuthResult {
    String getUserName();

    UserIdentity getUserIdentity();

    String getRemoteIp();

    static DorisAuthResult of(String userName, UserIdentity userIdentity, String remoteIp) {
        return ImmutableDorisAuthResult.builder()
                .userName(userName)
                .userIdentity(userIdentity)
                .remoteIp(remoteIp)
                .build();
    }
}
