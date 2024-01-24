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

package org.apache.doris.nereids.parser.plsql;

import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.BeginEndBlockContext;
import org.apache.doris.nereids.DorisParser.CallProcedureContext;
import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.DorisParser.CreateProcedureContext;
import org.apache.doris.nereids.DorisParser.CreateRoutineParamItemContext;
import org.apache.doris.nereids.DorisParser.DeclareCursorItemContext;
import org.apache.doris.nereids.DorisParser.DeclareVarItemContext;
import org.apache.doris.nereids.DorisParser.FetchStmtContext;
import org.apache.doris.nereids.DorisParser.OpenStmtContext;
import org.apache.doris.nereids.DorisParser.PrimitiveDataTypeContext;
import org.apache.doris.nereids.DorisParser.ProcedureBlockContext;
import org.apache.doris.nereids.DorisParser.ProcedureSelectContext;
import org.apache.doris.nereids.DorisParser.ProcedureStatementContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.nereids.parser.ParseDialect;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.parser.ParserUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.CallCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateProcedureCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.procedure.Var;
import org.apache.doris.procedure.Var.Type;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extends from {@link org.apache.doris.nereids.parser.LogicalPlanBuilder},
 * just focus on the difference between these query syntax.
 */
public class PLSqlLogicalPlanBuilder extends LogicalPlanBuilder {
    private final ParserContext parserContext;

    public PLSqlLogicalPlanBuilder() {
        this.parserContext = new ParserContext(ParseDialect.PLSQL_1_ALL);
    }

    @Override
    public Expression visitColumnReference(ColumnReferenceContext ctx) {
        Var var = ConnectContext.get().getProcedureExec().findVariable(ctx.getText());
        if (var != null && var.type == Type.EXPRESSION) {
            return (Expression) var.value;
        }
        return UnboundSlot.quoted(ctx.getText());
    }

    @Override
    public LogicalPlan visitCreateProcedure(CreateProcedureContext ctx) {
        return ParserUtils.withOrigin(ctx, () -> {
            // exec.functions.addUserProcedure(ctx);
            // addLocalUdf(ctx);                      // Add procedures as they can be invoked by functions

            LogicalPlan createProcedurePlan;
            String name = ctx.identifier(0).getText().toUpperCase();

            List<Map<String, DataType>> arguments = new ArrayList<>();
            for (CreateRoutineParamItemContext routineParamItem : ctx.createRoutineParams().createRoutineParamItem()) {
                String argName = routineParamItem.identifier().getText();
                if (!(routineParamItem.dataType() instanceof PrimitiveDataTypeContext)) {
                    throw new ParseException("Procedure parameter type not support ComplexDataType ", ctx);
                }
                DataType argType = visitPrimitiveDataType(((PrimitiveDataTypeContext) routineParamItem.dataType()));
                argType = argType.conversion();
                Map<String, DataType> arg = new HashMap<>();
                arg.put(argName, argType);
                arguments.add(arg);
            }
            // if (builtinFunctions.exists(name)) {
            //     exec.info(ctx, name + " is a built-in function which cannot be redefined.");
            //     return;
            // }
            // trace(ctx, "CREATE PROCEDURE " + name);
            // saveInCache(name, ctx);
            createProcedurePlan = new CreateProcedureCommand(name, getOriginSql(ctx), ctx.REPLACE() != null, ctx,
                    arguments);
            return createProcedurePlan;
        });
    }

    @Override
    public LogicalPlan visitCallProcedure(CallProcedureContext ctx) {
        String functionName = ctx.functionName.getText();
        List<Expression> arguments = ctx.expression().stream().<Expression>map(this::typedVisit)
                .collect(ImmutableList.toImmutableList());
        return new CallCommand(ctx, functionName, arguments);
    }

    /**
     * visitChildrenReal
     */
    public Integer visitChildrenReal(RuleNode node) {
        Integer result = null;
        int n = node.getChildCount();

        for (int i = 0; i < n && this.shouldVisitNextChild(node, result); ++i) {
            ParseTree c = node.getChild(i);
            Integer childResult = (Integer) c.accept(this);
            result = (Integer) this.aggregateResult(result, childResult);
        }

        return result;
    }

    @Override
    public Integer visitProcedureBlock(ProcedureBlockContext ctx) {
        return visitChildrenReal(ctx);
    }

    /**
     * Enter BEGIN-END block
     */
    @Override
    public Integer visitBeginEndBlock(BeginEndBlockContext ctx) {
        // enterScope(Scope.Type.BEGIN_END);
        // leaveScope();
        return visitChildrenReal(ctx);
        // return visit(ctx.block().procedureStatement(0).procedureSelect());
    }

    @Override
    public Integer visitBlock(DorisParser.BlockContext ctx) {
        return visitChildrenReal(ctx);
    }

    @Override
    public Object visitProcedureStatement(ProcedureStatementContext ctx) {
        return ConnectContext.get().getProcedureExec().visitStmt(ctx);
    }

    @Override
    public Object visitProcedureSelect(ProcedureSelectContext ctx) {
        return ConnectContext.get().getProcedureExec().select.select(ctx);
    }

    /**
     * DECLARE cursor statement
     */
    @Override
    public Integer visitDeclareCursorItem(DeclareCursorItemContext ctx) {
        return ConnectContext.get().getProcedureExec().stmt.declareCursor(ctx);
    }

    /**
     * DECLARE variable statement
     */
    @Override
    public Integer visitDeclareVarItem(DeclareVarItemContext ctx) {
        // TableClass userDefinedType = null;
        // Row row = null;
        // String len = null;
        // String scale = null;
        // Var defaultVar = null;
        // if (ctx.dtype().T_ROWTYPE() != null) {
        //     row = meta.getRowDataType(ctx, exec.conf.defaultConnection, ctx.dtype().qident().getText());
        //     if (row == null) {
        //         type = org.apache.doris.hplsql.Var.DERIVED_ROWTYPE;
        //     }
        // } else {
        // type = getDataType(ctx);

        if (!(ctx.dataType() instanceof PrimitiveDataTypeContext)) {
            throw new ParseException("Declare variable type not support ComplexDataType ", ctx);
        }
        DataType type = visitPrimitiveDataType(((PrimitiveDataTypeContext) ctx.dataType()));
        type = type.conversion();
        // if (ctx.dtype_len() != null) {
        //     len = ctx.dtype_len().L_INT(0).getText();
        //     if (ctx.dtype_len().L_INT(1) != null) {
        //         scale = ctx.dtype_len().L_INT(1).getText();
        //     }
        // }
        // if (ctx.dtype_default() != null) {
        //     defaultVar = evalPop(ctx.dtype_default());
        // }
        // userDefinedType = types.get(type);
        // if (userDefinedType != null) {
        //     type = org.apache.doris.hplsql.Var.Type.HPL_OBJECT.name();
        // }

        // }
        int cnt = ctx.identifier().size();        // Number of variables declared with the same data type and default
        for (int i = 0; i < cnt; i++) {
            String name = ctx.identifier(i).getText();
            // if (row == null) {
            Var var = new Var(name, type, (Integer) null, null, null);
            // if (userDefinedType != null && defaultVar == null) {
            //     var.setValue(userDefinedType.newInstance());
            // }
            ConnectContext.get().getProcedureExec().addVariable(var);
            if (ctx.CONSTANT() != null) {
                var.setConstant(true);
            }
            // if (trace) {
            //     if (defaultVar != null) {
            //         trace(ctx, "DECLARE " + name + " " + type + " = " + var.toSqlString());
            //     } else {
            //         trace(ctx, "DECLARE " + name + " " + type);
            //     }
            // }
            // } else {
            //     exec.addVariable(new org.apache.doris.hplsql.Var(name, row));
            //     if (trace) {
            //         trace(ctx, "DECLARE " + name + " " + ctx.dtype().getText());
            //     }
            // }
        }
        return 0;
    }

    /**
     * OPEN cursor statement
     */
    @Override
    public Integer visitOpenStmt(OpenStmtContext ctx) {
        return ConnectContext.get().getProcedureExec().stmt.open(ctx);
    }

    /**
     * FETCH cursor statement
     */
    @Override
    public Integer visitFetchStmt(FetchStmtContext ctx) {
        return ConnectContext.get().getProcedureExec().stmt.fetch(ctx);
    }
}
