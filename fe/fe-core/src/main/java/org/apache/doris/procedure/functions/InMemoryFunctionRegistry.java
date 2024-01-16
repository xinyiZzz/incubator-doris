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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/functions/InMemoryFunctionRegistry.java
// and modified by Doris

package org.apache.doris.procedure.functions;

import org.apache.doris.hplsql.HplsqlParser;
import org.apache.doris.hplsql.Var;
import org.apache.doris.hplsql.functions.BuiltinFunctions;
import org.apache.doris.hplsql.functions.FunctionRegistry;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.procedure.Exec;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HPL/SQL functions
 */
public class InMemoryFunctionRegistry implements FunctionRegistry { // hplsql-xinyi 这个还有用么 // 脚本执行的时候用这个
    Exec exec;
    private BuiltinFunctions builtinFunctions;
    HashMap<String, HplsqlParser.Create_function_stmtContext> funcMap = new HashMap<>();
    HashMap<String, HplsqlParser.Create_procedure_stmtContext> procMap = new HashMap<>();
    boolean trace = false;

    public InMemoryFunctionRegistry(Exec e, BuiltinFunctions builtinFunctions) {
        this.exec = e;
        // this.trace = exec.getTrace();
        this.builtinFunctions = builtinFunctions;
    }

    @Override
    public boolean exists(String name) {
        return funcMap.containsKey(name) || procMap.containsKey(name);
    }

    @Override
    public void remove(String name) {
        funcMap.remove(name);
        procMap.remove(name);
    }

    @Override
    public boolean exec(String name, HplsqlParser.Expr_func_paramsContext ctx) {
        if (builtinFunctions.exec(name, ctx)) {
            return true;
        }
        if (execFunction(name, ctx)) {
            return true;
        }
        return (procMap.get(name) != null && execProc(name, ctx));
    }

    /**
     * Execute a user-defined function
     */
    private boolean execFunction(String name, HplsqlParser.Expr_func_paramsContext ctx) {
        // HplsqlParser.Create_function_stmtContext userCtx = funcMap.get(name);
        // if (userCtx == null) {
        //     return false;
        // }
        // if (trace) {
        //     trace(ctx, "EXEC FUNCTION " + name);
        // }
        // ArrayList<Var> actualParams = getActualCallParameters(ctx);
        // exec.enterScope(Scope.Type.ROUTINE);
        // setCallParameters(name, ctx, actualParams, userCtx.create_routine_params(), null, exec);
        // if (userCtx.declare_block_inplace() != null) {
        //     visit(userCtx.declare_block_inplace());
        // }
        // visit(userCtx.single_block_stmt());
        // exec.leaveScope();
        return true;
    }

    /**
     * Execute a stored procedure using CALL or EXEC statement passing parameters
     */
    private boolean execProc(String name, HplsqlParser.Expr_func_paramsContext ctx) {
        // if (trace) {
        //     trace(ctx == null ? null : ctx.getParent(), "EXEC PROCEDURE " + name);
        // }
        // HplsqlParser.Create_procedure_stmtContext procCtx = procMap.get(name);
        // if (procCtx == null) {
        //     trace(ctx.getParent(), "Procedure not found");
        //     return false;
        // }
        // ArrayList<Var> actualParams = getActualCallParameters(ctx);
        // HashMap<String, Var> out = new HashMap<>();
        // exec.enterScope(Scope.Type.ROUTINE);
        // exec.callStackPush(name);
        // if (procCtx.declare_block_inplace() != null) {
        //     visit(procCtx.declare_block_inplace());
        // }
        // if (procCtx.create_routine_params() != null) {
        //     setCallParameters(name, ctx, actualParams, procCtx.create_routine_params(), out, exec);
        // }
        // visit(procCtx.proc_block());
        // exec.callStackPop();
        // exec.leaveScope();
        // for (Map.Entry<String, Var> i : out.entrySet()) {      // Set OUT parameters // 和prepare statement有关，先不用管
        //     exec.setVariable(i.getKey(), i.getValue());
        // }
        return true;
    }

    /**
     * Set parameters for user-defined function call
     */
    public static void setCallParameters(String procName, List<Expression> callArguments,
            List<Map<String, DataType>> procedureArguments, HashMap<String, Expression> out, Exec exec) {
        if (callArguments == null || procedureArguments == null) {
            return;
        }
        int callArgumentsCnt = callArguments.size();
        int procedureArgumentsCnt = procedureArguments.size();
        if (callArgumentsCnt != procedureArgumentsCnt) {
            throw new RuntimeException("Procedure " + procName + " parameters lengths " + callArgumentsCnt
                    + " not equal to call parameters lengths " + procedureArgumentsCnt);
        }
        for (int i = 0; i < callArgumentsCnt; i++) {
            // HplsqlParser.ExprContext a = actual.func_param(i).expr();
            Map<String, DataType> procedureArgument = getCallParameter(callArguments, procedureArguments, i);
            String name = procedureArgument.keySet().iterator().next();
            DataType type = procedureArgument.get(name);
            // String len = null;
            // String scale = null;
            // if (p.dtype_len() != null) {
            //     len = p.dtype_len().L_INT(0).getText();
            //     if (p.dtype_len().L_INT(1) != null) {
            //         scale = p.dtype_len().L_INT(1).getText();
            //     }
            // }
            setCallParameter(name, TypeCoercionUtils.castIfNotSameType(callArguments.get(i), type), exec);
            // exec.trace(actual, "SET PARAM " + name + " = " + var.toString());
            // if (out != null && a.expr_atom() != null && a.expr_atom().qident() != null && (p.T_OUT() != null
            //         || p.T_INOUT() != null)) {
            //     String actualName = a.expr_atom().qident().getText();
            //     if (actualName != null) {
            //         out.put(actualName, var);
            //     }
            // }
        }
    }

    /**
     * Create a function or procedure parameter and set its value
     */
    static Expression setCallParameter(String name, Expression callArgument, Exec exec) {
        // TableClass hplClass = exec.getType(typeName); // 优先匹配表名
        // Var var = new Var(name, hplClass == null ? typeName : Var.Type.HPL_OBJECT.name(), len, scale, null);
        // if (hplClass != null) {
        //     var.setValue(hplClass.newInstance());
        // }
        // var.cast(value); // 设置变量value
        // exec.addVariable(var); // 把变量添加到exec
        // return var;
        // callArgument.get
        Alias var = new Alias(callArgument, name);
        exec.addVariable(var);
        return callArgument;
    }

    /**
     * Get call parameter definition by name (if specified) or position
     */
    static Map<String, DataType> getCallParameter(List<Expression> callArguments,
            List<Map<String, DataType>> procedureArguments, int pos) {
        int outPos = pos;
        // Expression callArgument = callArguments.get(pos);
        // if (!callArgument.alias().getName().equals("literal")) {
        //     // 如果call arg有名字，则找到对应名字的参数而不是相同位置
        //     int cnt = procedureArguments.size();
        //     for (int i = 0; i < cnt; i++) {
        //         if (callArgument.getExpressionName().equalsIgnoreCase(procedureArguments.get(i).getExpressionName())
        //         && callArgument.getDataType().equals(procedureArguments.get(i).getDataType())) {
        //             outPos = i;
        //             break;
        //         }
        //     }
        // }
        // return createRoutineParamsContext.createRoutineParamItem(outPos);
        // if (!callArguments.get(outPos).getDataType().equals(procedureArguments.get(outPos).getDataType())) {
        //     return null; // need throw
        // }
        return procedureArguments.get(outPos);
    }

    /**
     * Evaluate actual call parameters
     */
    public ArrayList<Var> getActualCallParameters(HplsqlParser.Expr_func_paramsContext actual) {
        if (actual == null || actual.func_param() == null) {
            return null;
        }
        int cnt = actual.func_param().size();
        ArrayList<Var> values = new ArrayList<>(cnt);
        for (int i = 0; i < cnt; i++) {
            values.add(evalPop(actual.func_param(i).expr()));
        }
        return values;
    }

    @Override
    public void addUserFunction(HplsqlParser.Create_function_stmtContext ctx) {
        // String name = ctx.ident().getText().toUpperCase();
        // if (builtinFunctions.exists(name)) {
        //     exec.info(ctx, name + " is a built-in function which cannot be redefined.");
        //     return;
        // }
        // if (trace) {
        //     trace(ctx, "CREATE FUNCTION " + name);
        // }
        // funcMap.put(name.toUpperCase(), ctx);
    }

    @Override
    public void addUserProcedure(HplsqlParser.Create_procedure_stmtContext ctx) {
        // String name = ctx.ident(0).getText().toUpperCase();
        // if (builtinFunctions.exists(name)) {
        //     exec.info(ctx, name + " is a built-in function which cannot be redefined.");
        //     return;
        // }
        // if (trace) {
        //     trace(ctx, "CREATE PROCEDURE " + name);
        // }
        // procMap.put(name.toUpperCase(), ctx);
    }

    /**
     * Evaluate the expression and pop value from the stack
     */
    private Var evalPop(ParserRuleContext ctx) {
        // exec.visit(ctx);
        // return exec.stackPop();
        return null;
    }

    /**
     * Execute rules
     */
    // private Integer visit(ParserRuleContext ctx) {
    //     return exec.visit(ctx);
    // }
    //
    // private void trace(ParserRuleContext ctx, String message) {
    //     if (trace) {
    //         exec.trace(ctx, message);
    //     }
    // }
}
