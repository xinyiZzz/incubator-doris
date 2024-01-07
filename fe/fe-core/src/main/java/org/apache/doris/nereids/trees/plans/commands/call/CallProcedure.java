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

package org.apache.doris.nereids.trees.plans.commands.call;

import org.apache.doris.hplsql.executor.DorisQueryExecutor;
import org.apache.doris.hplsql.store.MetaClient;
import org.apache.doris.hplsql.store.StoredProcedure;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Objects;

/**
 * CallProcedure
 */
public class CallProcedure extends CallFunc {
    private StoredProcedure proc;
    private List<Expression> args;
    private DorisQueryExecutor queryExecutor;

    private CallProcedure(StoredProcedure proc, List<Expression> args) {
        this.proc = Objects.requireNonNull(proc, "user is missing");
        this.args = Objects.requireNonNull(args, "catalogName is missing");
        this.queryExecutor = new DorisQueryExecutor();
    }

    /**
     * Create a CallFunc
     */
    public static CallFunc create(String funcName, ConnectContext ctx, List<Expression> args) {
        MetaClient client = new MetaClient();
        StoredProcedure proc = client.getStoredProcedure(funcName,
                ctx.getCurrentCatalog().getName(), ctx.getDatabase());
        return new CallProcedure(proc, args);
    }

    @Override
    public void run() {
        queryExecutor.executeQuery(proc.getSource(), null);
    }
}
