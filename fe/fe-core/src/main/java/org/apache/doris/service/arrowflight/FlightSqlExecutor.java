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

package org.apache.doris.service.arrowflight;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TUniqueId;

import java.sql.Statement;
import java.util.UUID;

public class FlightSqlExecutor {

    public static void executeQuery(FlightStatementContext<Statement> flightStatementContext) {
        try (AutoCloseConnectContext r = FlightSqlExecutor.buildConnectContext()) {
            StmtExecutor stmtExecutor = new StmtExecutor(r.connectContext, flightStatementContext.getQuery());
            r.connectContext.setExecutor(stmtExecutor);
            stmtExecutor.executeArrowFlightQuery(flightStatementContext);
        }
    }

    public static AutoCloseConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.internalSession = true;
        sessionVariable.setEnableNereidsPlanner(false);
        connectContext.setEnv(Env.getCurrentEnv());
        // connectContext.setDatabase(FeConstants.INTERNAL_DB_NAME);
        connectContext.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        connectContext.setQueryId(queryId);
        connectContext.setStartTime();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        connectContext.setResultSinkType(TResultSinkType.ARROW_FLIGHT_PROTOCAL);
        return new AutoCloseConnectContext(connectContext);
    }
}
