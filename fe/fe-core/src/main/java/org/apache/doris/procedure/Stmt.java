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

package org.apache.doris.procedure;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.hplsql.exception.QueryException;
import org.apache.doris.hplsql.executor.QueryExecutor;
import org.apache.doris.hplsql.executor.QueryResult;
import org.apache.doris.nereids.DorisParser.DeclareCursorItemContext;
import org.apache.doris.nereids.DorisParser.FetchStmtContext;
import org.apache.doris.nereids.DorisParser.OpenStmtContext;
import org.apache.doris.procedure.Var.Type;

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
                            // TODO 应该生成 doris 常量 expression，后面SQL中使用变量
                            var.setExpressionValue(queryResult, i); // 将每一列的值 set 到变量中
                        } else {
                            var.setRowValues(queryResult); // ？
                        }
                        // if (trace) {
                        //     trace(ctx, var, queryResult.metadata(), i);
                        // }
                        // } else if (trace) {
                        //     trace(ctx, "Variable not found: " + ctx.ident(i + 1).getText());
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
}
