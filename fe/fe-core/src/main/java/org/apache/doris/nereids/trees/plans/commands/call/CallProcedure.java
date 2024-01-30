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

import org.apache.doris.plsql.executor.PlsqlQueryExecutor;
import org.apache.doris.qe.ConnectContext;

import java.util.Objects;

/**
 * CallProcedure
 */
public class CallProcedure extends CallFunc {
    private final PlsqlQueryExecutor executor;
    private final ConnectContext ctx;
    private final String source;

    private CallProcedure(PlsqlQueryExecutor executor, ConnectContext ctx, String source) {
        this.executor = Objects.requireNonNull(executor, "executor is missing");
        this.ctx = ctx;
        this.source = source;
    }

    /**
     * Create a CallFunc
     */
    public static CallFunc create(ConnectContext ctx, String source) {
        PlsqlQueryExecutor plsqlQueryExecutor = ctx.getHplsqlQueryExecutor(); // 这段逻辑，移到 Mysql processer
        if (plsqlQueryExecutor == null) { // hplsql, 这是为啥
            plsqlQueryExecutor = new PlsqlQueryExecutor();
            ctx.setHplsqlQueryExecutor(plsqlQueryExecutor); // hplsql, 这些逻辑放到 hpl connect processor
        }
        return new CallProcedure(plsqlQueryExecutor, ctx, source);
    }

    @Override
    public void run() {
        executor.execute(ctx, source);
    }
}
