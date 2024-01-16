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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/functions/HmsFunctionRegistry.java
// and modified by Doris

package org.apache.doris.procedure.functions;

import org.apache.doris.hplsql.HplsqlParser;
import org.apache.doris.hplsql.functions.BuiltinFunctions;
import org.apache.doris.hplsql.store.MetaClient;
import org.apache.doris.hplsql.store.StoredProcedure;
import org.apache.doris.nereids.DorisParser.CreateProcedureContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.CreateProcedureCommand;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.procedure.Exec;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DorisFunctionRegistry implements FunctionRegistry {
    private Exec exec;
    private boolean trace;
    private MetaClient client;
    private BuiltinFunctions builtinFunctions;
    private Map<String, ParserRuleContext> cache = new HashMap<>();

    public DorisFunctionRegistry(Exec e, MetaClient client, BuiltinFunctions builtinFunctions) {
        this.exec = e;
        this.client = client;
        this.builtinFunctions = builtinFunctions;
        // this.trace = exec.getTrace();
    }

    @Override
    public boolean exists(String name) {
        return isCached(name) || getProc(name).isPresent();
    }

    @Override
    public void remove(String name) {
        try {
            client.dropStoredProcedure(name, ConnectContext.get().getCurrentCatalog().getName(),
                    ConnectContext.get().getDatabase());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isCached(String name) {
        return cache.containsKey(qualified(name));
    }

    private String qualified(String name) {
        return (ConnectContext.get().getDatabase() + "." + name).toUpperCase();
    }


    @Override
    public boolean exec(String name, List<Expression> callArguments, ConnectContext connectContext) {
        // if (builtinFunctions.exec(name, ctx)) { // 内置函数
        //     return true;
        // }
        // if (isCached(name)) {
        //     trace(ctx, "EXEC CACHED FUNCTION " + name);
        //     execProcOrFunc(ctx, cache.get(qualified(name)), name);
        //     return true;
        // }
        Optional<StoredProcedure> proc = getProc(name);
        if (proc.isPresent()) {
            // trace(ctx, "EXEC HMS FUNCTION " + name);
            CreateProcedureCommand createProcedureCommand = parse(proc.get(), connectContext);
            execProcOrFunc(createProcedureCommand.getCreateProcedureContext(), name, callArguments,
                    createProcedureCommand.getArguments());
            // saveInCache(name, procCtx);
            return true;
        }
        return false;
    }

    /**
     * Execute a stored procedure using CALL or EXEC statement passing parameters
     */
    private void execProcOrFunc(ParserRuleContext procCtx, String name, List<Expression> callArguments,
            List<Map<String, DataType>> procedureArguments) {
        // exec.callStackPush(name);
        HashMap<String, Expression> out = new HashMap<>();
        // ArrayList<Var> actualParams = getActualCallParameters(ctx);
        // exec.enterScope(Scope.Type.ROUTINE);
        callWithParameters(procCtx, out, callArguments, procedureArguments);
        // exec.callStackPop();
        // exec.leaveScope();
        // for (Map.Entry<String, Var> i : out.entrySet()) { // Set OUT parameters
        //     exec.setVariable(i.getKey(), i.getValue());
        // }
    }

    private void callWithParameters(ParserRuleContext procCtx, HashMap<String, Expression> out,
            List<Expression> callArguments, List<Map<String, DataType>> procedureArguments) {
        // if (procCtx instanceof HplsqlParser.Create_function_stmtContext) {
        //     HplsqlParser.Create_function_stmtContext func = (HplsqlParser.Create_function_stmtContext) procCtx;
        //     InMemoryFunctionRegistry.setCallParameters(func.ident().getText(), ctx, actualParams,
        //             func.create_routine_params(), null, exec);
        //     if (func.declare_block_inplace() != null) {
        //         exec.visit(func.declare_block_inplace());
        //     }
        //     exec.visit(func.single_block_stmt());
        // } else {
        CreateProcedureContext proc = (CreateProcedureContext) procCtx;
        InMemoryFunctionRegistry.setCallParameters(proc.identifier(0).getText(), callArguments, procedureArguments, out,
                exec);

        LogicalPlanBuilder logicalPlanBuilder = new LogicalPlanBuilder(); // 每次new一个新的logicalPlanBuilder 无状态
        logicalPlanBuilder.visit(proc.procBlock());
        // }
    }

    private CreateProcedureCommand parse(StoredProcedure proc, ConnectContext connectContext) {

        // HplsqlLexer lexer = new HplsqlLexer(new ANTLRInputStream(proc.getSource()));
        // CommonTokenStream tokens = new CommonTokenStream(lexer);
        // HplsqlParser parser = new HplsqlParser(tokens);
        // org.apache.doris.hplsql.functions.DorisFunctionRegistry.ProcVisitor
        //         visitor = new org.apache.doris.hplsql.functions.DorisFunctionRegistry.ProcVisitor();
        // parser.program().accept(visitor);

        StmtExecutor executor = new StmtExecutor(connectContext, proc.getSource());
        executor.parseByNereids();
        // return visitor.func != null ? visitor.func : visitor.proc;
        // return logicalPlan.getCreateProcedureContext();
        return (CreateProcedureCommand) ((LogicalPlanAdapter) executor.getParsedStmt()).getLogicalPlan();
    }

    private Optional<StoredProcedure> getProc(String name) {
        return Optional.ofNullable(client.getStoredProcedure(name, ConnectContext.get().getCurrentCatalog().getName(),
                ConnectContext.get().getDatabase()));
    }

    // private ArrayList<Var> getActualCallParameters(HplsqlParser.Expr_func_paramsContext actual) {
    //     if (actual == null || actual.func_param() == null) {
    //         return null;
    //     }
    //     int cnt = actual.func_param().size();
    //     ArrayList<Var> values = new ArrayList<>(cnt);
    //     for (int i = 0; i < cnt; i++) {
    //         values.add(evalPop(actual.func_param(i).expr()));
    //     }
    //     return values;
    // }

    @Override
    public void addUserFunction(HplsqlParser.Create_function_stmtContext ctx) {
        // String name = ctx.ident().getText().toUpperCase();
        // if (builtinFunctions.exists(name)) {
        //     // exec.info(ctx, name + " is a built-in function which cannot be redefined.");
        //     return;
        // }
        // trace(ctx, "CREATE FUNCTION " + name);
        // saveInCache(name, ctx);
        // saveStoredProc(name, Exec.getFormattedText(ctx), ctx.T_REPLACE() != null);
    }

    @Override
    public void addUserProcedure(HplsqlParser.Create_procedure_stmtContext ctx) {
        // String name = ctx.ident(0).getText().toUpperCase();
        // if (builtinFunctions.exists(name)) {
        //     // exec.info(ctx, name + " is a built-in function which cannot be redefined.");
        //     return;
        // }
        // trace(ctx, "CREATE PROCEDURE " + name);
        // saveInCache(name, ctx);
        // saveStoredProc(name, Exec.getFormattedText(ctx),  ctx.T_REPLACE() != null);
    }

    // private void saveStoredProc(String name, String source, boolean isForce) {
    //     client.addStoredProcedure(name, ConnectContext.get().getCurrentCatalog().getName(),
    //             ConnectContext.get().getDatabase(),
    //             ConnectContext.get().getQualifiedUser(), source, isForce);
    // }
    //
    // private void saveInCache(String name, ParserRuleContext procCtx) {
    //     cache.put(qualified(name), procCtx);
    // }

    // /**
    //  * Evaluate the expression and pop value from the stack
    //  */
    // private Var evalPop(ParserRuleContext ctx) {
    //     exec.visit(ctx);
    //     return exec.stackPop();
    // }
    //
    // private void trace(ParserRuleContext ctx, String message) {
    //     if (trace) {
    //         exec.trace(ctx, message);
    //     }
    // }

    // private static class ProcVisitor extends HplsqlBaseVisitor<Void> {
    //     HplsqlParser.Create_function_stmtContext func;
    //     HplsqlParser.Create_procedure_stmtContext proc;
    //
    //     @Override
    //     public Void visitCreate_procedure_stmt(HplsqlParser.Create_procedure_stmtContext ctx) {
    //         proc = ctx;
    //         return null;
    //     }
    //
    //     @Override
    //     public Void visitCreate_function_stmt(HplsqlParser.Create_function_stmtContext ctx) {
    //         func = ctx;
    //         return null;
    //     }
    // }
}
