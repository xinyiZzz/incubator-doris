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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.hplsql.Conf;
import org.apache.doris.hplsql.executor.DorisQueryExecutor;
import org.apache.doris.hplsql.executor.HplsqlResult;
import org.apache.doris.nereids.DorisParser.CallProcedureContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.procedure.Exec;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * call func()
 */
public class CallCommand extends Command implements ForwardWithSync {
    public static final Logger LOG = LogManager.getLogger(CallCommand.class);

    // private final UnboundFunction unboundFunction;
    private CallProcedureContext ctx;
    private String functionName;
    private List<Expression> arguments;

    // /**
    //  * constructor
    //  */
    // public CallCommand(UnboundFunction unboundFunction, CallProcedureContext stmt) {
    //     super(PlanType.CALL_COMMAND);
    //     this.unboundFunction = Objects.requireNonNull(unboundFunction, "function is null");
    //     this.stmt = Objects.requireNonNull(stmt, "stmt is null");
    // }

    /**
     * constructor
     */
    public CallCommand(CallProcedureContext ctx, String functionName, List<Expression> arguments) {
        super(PlanType.CALL_COMMAND);
        this.ctx = ctx;
        this.functionName = functionName;
        this.arguments = arguments;
    }

    @Override
    public void run(ConnectContext connectContext, StmtExecutor executor) throws Exception {
        // CallFunc analyzedFunc = CallFunc.getFunc(ctx, ctx.getCurrentUserIdentity(), unboundFunction, stmt);
        // analyzedFunc.run();
        connectContext.setRunProcedure(true);
        connectContext.getSessionVariable().enableFallbackToOriginalPlanner = false;
        HplsqlResult result = new HplsqlResult();
        Exec exec = new Exec(new Conf(), result, new DorisQueryExecutor(), result);
        exec.init();
        connectContext.setProcedureExec(exec);
        exec.visitCall_stmt(connectContext, ctx, functionName, arguments);
        connectContext.getMysqlChannel().reset();
        // connectContext.getExecutor().sendEmptyFields();
        connectContext.getState().setOk();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCallCommand(this, context);
    }

}
