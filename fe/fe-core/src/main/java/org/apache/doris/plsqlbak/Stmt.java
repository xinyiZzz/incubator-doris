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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Stmt.java
// and modified by Doris

package org.apache.doris.plsqlbak;

import org.apache.doris.hplsql.executor.QueryExecutor;

import org.antlr.v4.runtime.ParserRuleContext;

/**
 * HPL/SQL statements execution
 */
public class Stmt {
    Exec exec = null;
    // Stack<Var> stack = null;
    // Conf conf;
    // Meta meta;
    // Console console;

    // boolean trace = false;
    private QueryExecutor queryExecutor;

    Stmt(Exec e, QueryExecutor queryExecutor) {
        exec = e;
        // stack = exec.getStack();
        // conf = exec.getConf();
        // meta = exec.getMeta();
        // trace = exec.getTrace();
        // console = exec.console;
        this.queryExecutor = queryExecutor;
    }

    /**
     * DECLARE cursor statement
     */
    public Integer declareCursor(DeclareCursorItemContext ctx) {
        String name = ctx.identifier().getText();
        // if (trace) {
        //     trace(ctx, "DECLARE CURSOR " + name);
        // }
        Cursor cursor = new Cursor(null);
        if (ctx.expression() != null) {
            cursor.setExprCtx(ctx.expression());
        } else if (ctx.query() != null) {
            cursor.setSelectCtx(ctx.query());
        }
        // if (ctx.cursor_with_return() != null) {
        //     cursor.setWithReturn(true);
        // }
        Var var = new Var(name, Type.CURSOR, cursor);
        exec.addVariable(var);
        return 0;
    }

    /**
     * OPEN cursor statement
     */
    public Integer open(OpenStmtContext ctx) {
        // trace(ctx, "OPEN");
        Cursor cursor = null;
        Var var = null;
        String cursorName = ctx.identifier().getText();
        // String sql = null;
        // if (ctx.FOR() != null) {                             // SELECT statement or dynamic SQL
        //     sql = ctx.expr() != null ? evalPop(ctx.expr()).toString() : evalPop(ctx.select_stmt()).toString();
        //     cursor = new Cursor(sql);
        //     var = exec.findCursor(cursorName);                      // Can be a ref cursor variable
        //     if (var == null) {
        //         var = new Var(cursorName, Var.Type.CURSOR, cursor);
        //         exec.addVariable(var);
        //     } else {
        //         var.setValue(cursor);
        //     }
        // } else {                                                 // Declared cursor
        var = exec.findVariable(cursorName);
        if (var != null && var.type == Var.Type.CURSOR) {
            cursor = (Cursor) var.value;
            if (cursor.getSqlExpr() != null) {
                // sql = evalPop(cursor.getSqlExpr()).toString();
                cursor.setSql(exec.logicalPlanBuilder.getOriginSql(cursor.getSqlExpr()));
            } else if (cursor.getSqlSelect() != null) {
                // sql = evalPop(cursor.getSqlSelect()).toString();
                cursor.setSql(exec.logicalPlanBuilder.getOriginSql(cursor.getSqlSelect()));
            }
        }
        // }
        if (cursor != null) {
            // if (trace) {
            //     trace(ctx, cursorName + ": " + sql);
            // }
            cursor.open(queryExecutor, ctx);
            // QueryResult queryResult = cursor.getQueryResult();
            // if (queryResult.error()) {
            //     exec.signal(queryResult);
            //     return 1;
            // } else if (!exec.getOffline()) {
            //     exec.setSqlCode(SqlCodes.SUCCESS);
            // }
            // if (cursor.isWithReturn()) {
            //     exec.addReturnCursor(var);
            // }
        } else {
            // trace(ctx, "Cursor not found: " + cursorName);
            // exec.setSqlCode(SqlCodes.ERROR);
            // exec.signal(Signal.Type.SQLEXCEPTION);
            return 1;
        }
        return 0;
    }

    /**
     * FETCH cursor statement
     */
    public Integer fetch(FetchStmtContext ctx) {
        // trace(ctx, "FETCH");
        String name = ctx.identifier(0).getText();
        Var varCursor = exec.findCursor(name);
        if (varCursor == null) {
            // trace(ctx, "Cursor not found: " + name);
            // exec.setSqlCode(SqlCodes.ERROR);
            // exec.signal(Signal.Type.SQLEXCEPTION);
            return 1;
        } else if (varCursor.value == null) {
            // trace(ctx, "Cursor not open: " + name);
            // exec.setSqlCode(SqlCodes.ERROR);
            // exec.signal(Signal.Type.SQLEXCEPTION);
            return 1;
        } else if (exec.getOffline()) {
            // exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
            // exec.signal(Signal.Type.NOTFOUND);
            return 0;
        }
        // Assign values from the row to local variables
        try {
            Cursor cursor = (Cursor) varCursor.value;
            int cols = ctx.identifier().size() - 1;
            QueryResult queryResult = cursor.getQueryResult();

            // if (ctx.bulk_collect_clause() != null) {
            //     long limit = ctx.fetch_limit() != null ? evalPop(ctx.fetch_limit().expr()).longValue() : -1;
            //     long rowIndex = 1;
            //     List<Table> tables = exec.intoTables(ctx, intoVariableNames(ctx, cols));
            //     tables.forEach(Table::removeAll);
            //     while (queryResult.next()) {
            //         cursor.setFetch(true);
            //         for (int i = 0; i < cols; i++) {
            //             Table table = tables.get(i);
            //             table.populate(queryResult, rowIndex, i);
            //         }
            //         rowIndex++;
            //         if (limit != -1 && rowIndex - 1 >= limit) {
            //             break;
            //         }
            //     }
            // } else {
            if (queryResult.next()) {
                cursor.setFetch(true);
                for (int i = 0; i < cols; i++) {
                    Var var = exec.findVariable(ctx.identifier(i + 1).getText()); // FETCH into 后面的变量
                    if (var != null) { // 变量要提前DECLARE定义
                        if (var.type != Var.Type.ROW) {
                            var.setExpressionValue(queryResult, i); // 将每一列的值 set 到变量中
                        } else {
                            var.setRowValues(queryResult); // ？
                        }
                        // if (trace) {
                        //     trace(ctx, var, queryResult.metadata(), i);
                        // }
                        // } else if (trace) {
                        //     trace(ctx, "Variable not found: " + ctx.ident_pl(i + 1).getText());
                    }
                }
                // exec.incRowCount();
                // exec.setSqlSuccess();
            } else {
                cursor.setFetch(false);
                // exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
            }
            // }
        } catch (QueryException e) {
            // exec.setSqlCode(e);
            // exec.signal(Signal.Type.SQLEXCEPTION, e.getMessage(), e);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    /**
     * CLOSE cursor statement
     */
    public Integer close(CloseStmtContext ctx) {
        // trace(ctx, "CLOSE");
        String name = ctx.identifier().getText();
        Var var = exec.findVariable(name);
        if (var != null && var.type == Var.Type.CURSOR) {
            ((Cursor) var.value).close();
            // exec.setSqlCode(SqlCodes.SUCCESS);
            // } else if (trace) {
            //     trace(ctx, "Cursor not found: " + name);
        }
        return 0;
    }

    /**
     * IF statement (PL/SQL syntax)
     */
    public Integer ifPlsql(IfPlsqlStmtContext ctx) {
        // boolean trueExecuted = false;
        // // trace(ctx, "IF");
        // Expression expr = exec.logicalPlanBuilder.getExpression(ctx.booleanExpression());
        // if (expr instanceof BinaryOperator) {
        //     BinaryOperator condition = (BinaryOperator) expr;
        //     condition.
        // } else {
        //
        // }
        //
        // // Map<Expression, Expression> replaceMap =
        // //         filter.child().getOutputs().stream().filter(e -> e instanceof Alias)
        // //                 .collect(Collectors.toMap(NamedExpression::toSlot, e -> ((Alias) e).child()));
        //
        // Expression foldExpression =
        //         FoldConstantRule.INSTANCE.rewrite(newExpr, context);
        //
        // if (foldExpression == BooleanLiteral.FALSE) {
        //     return new LogicalEmptyRelation(
        //             ctx.statementContext.getNextRelationId(), filter.getOutput());
        // } else if (foldExpression != BooleanLiteral.TRUE) {
        //     newConjuncts.add(expression);
        // }
        //
        // if (evalPop(ctx.booleanExpression()).isTrue()) {
        //     // trace(ctx, "IF TRUE executed");
        //     visit(ctx.block());
        //     trueExecuted = true;
        // } else if (ctx.elseif_block() != null) {
        //     int cnt = ctx.elseif_block().size();
        //     for (int i = 0; i < cnt; i++) {
        //         if (evalPop(ctx.elseif_block(i).bool_expr()).isTrue()) {
        //             trace(ctx, "ELSE IF executed");
        //             visit(ctx.elseif_block(i).block());
        //             trueExecuted = true;
        //             break;
        //         }
        //     }
        // }
        // if (!trueExecuted && ctx.else_block() != null) {
        //     trace(ctx, "ELSE executed");
        //     visit(ctx.else_block());
        // }
        return 0;
    }

    /**
     * Check if an exception is raised or EXIT executed, and we should leave the block
     */
    boolean canContinue(String label) {
        Signal signal = exec.signalPeek();
        if (signal != null && signal.type == Signal.Type.SQLEXCEPTION) {
            return false;
        }
        signal = exec.signalPeek();
        if (signal != null && signal.type == Signal.Type.LEAVE_LOOP) {
            if (signal.value == null || signal.value.isEmpty() || (label != null && label.equalsIgnoreCase(
                    signal.value))) {
                exec.signalPop();
            } // why?
            return false;
        }
        return true;
    }

    public Integer unconditionalLoop(UnconditionalLoopStmtContext ctx) {
        // trace(ctx, "UNCONDITIONAL LOOP - ENTERED");
        String label = exec.labelPop();
        do {
            exec.enterScope(Scope.Type.LOOP);
            visit(ctx.block());
            exec.leaveScope();
        } while (canContinue(label));
        // trace(ctx, "UNCONDITIONAL LOOP - LEFT");
        return 0;
    }

    /**
     * Leave the specified or innermost loop unconditionally
     */
    public void leaveLoop(String value) {
        exec.signal(Signal.Type.LEAVE_LOOP, value);
    }

    /**
     * LEAVE statement (leave the specified loop unconditionally)
     */
    public Integer leave(LeaveStmtContext ctx) {
        // trace(ctx, "LEAVE");
        String label = "";
        if (ctx.identifier() != null) {
            label = ctx.identifier().getText();
        }
        leaveLoop(label);
        return 0;
    }

    /**
     * Evaluate the expression and pop value from the stack
     */
    Var evalPop(ParserRuleContext ctx) {
        ctx.accept(exec.logicalPlanBuilder);
        if (!exec.stack.isEmpty()) {
            return exec.stackPop();
        }
        return Var.Empty;
    }

    Var evalPop(ParserRuleContext ctx, long def) {
        if (ctx != null) {
            ctx.accept(exec.logicalPlanBuilder);
            return exec.stackPop();
        }
        return new Var(def);
    }

    /**
     * Execute rules
     */
    Object visit(ParserRuleContext ctx) {
        return ctx.accept(exec.logicalPlanBuilder);
    }
}
