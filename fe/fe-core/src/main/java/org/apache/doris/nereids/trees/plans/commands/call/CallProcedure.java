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
import org.apache.doris.hplsql.executor.HplsqlQueryExecutor;
import org.apache.doris.hplsql.store.MetaClient;
import org.apache.doris.hplsql.store.StoredProcedure;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.List;
import java.util.Objects;

/**
 * CallProcedure
 */
public class CallProcedure extends CallFunc {
    // private StoredProcedure proc;
    private HplsqlQueryExecutor executor;
    private List<Expression> args;
    private DorisQueryExecutor queryExecutor;
    private String stmt;
    // private ResultListener resultListener = ResultListener.NONE;

    private CallProcedure(HplsqlQueryExecutor executor, List<Expression> args, String stmt) {
        this.executor = Objects.requireNonNull(executor, "executor is missing");
        this.args = Objects.requireNonNull(args, "args is missing");
        this.queryExecutor = new DorisQueryExecutor();
        this.stmt = stmt;
        // resultListener = new HplsqlResult(processor);
    }

    private static String getOriginSql(ParserRuleContext ctx) {
        int startIndex = ctx.start.getStartIndex();
        int stopIndex = ctx.stop.getStopIndex();
        org.antlr.v4.runtime.misc.Interval interval = new org.antlr.v4.runtime.misc.Interval(startIndex, stopIndex);
        return ctx.start.getInputStream().getText(interval);
    }

    /**
     * Create a CallFunc
     */
    public static CallFunc create(String funcName, ConnectContext ctx, List<Expression> args) {
        MetaClient client = new MetaClient();
        StoredProcedure proc = client.getStoredProcedure(funcName, ctx.getCurrentCatalog().getName(),
                ctx.getDatabase());
        // StmtExecutor executor = new StmtExecutor(ctx, proc.getSource());
        // executor.parseByNereids();
        // CreateProcedureCommand logicalPlan
        //         = (CreateProcedureCommand) ((LogicalPlanAdapter) executor.getParsedStmt()).getLogicalPlan();
        // return new CallProcedure(proc, args,
        //         getOriginSql(logicalPlan.getCreateProcedureContext().procBlock().beginEndBlock().block()));

        HplsqlQueryExecutor hplsqlQueryExecutor = ctx.getHplsqlQueryExecutor(); // 这段逻辑，移到 Mysql processer
        if (hplsqlQueryExecutor == null) { // hplsql, 这是为啥
            hplsqlQueryExecutor = new HplsqlQueryExecutor();
            ctx.setHplsqlQueryExecutor(hplsqlQueryExecutor); // hplsql, 这些逻辑放到 hpl connect processor
        }
        return new CallProcedure(hplsqlQueryExecutor, args, proc.getSource());
    }

    @Override
    public void run() {
        // QueryResult query = queryExecutor.executeQuery(stmt, null);
        executor.execute(stmt);
    }
}
