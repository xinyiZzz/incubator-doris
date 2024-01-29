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

package org.apache.doris.hplsql;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.PLParserParser.Allocate_cursor_stmtContext;
import org.apache.doris.nereids.PLParserParser.Associate_locator_stmtContext;
import org.apache.doris.nereids.PLParserParser.Declare_cursor_itemContext;
import org.apache.doris.nereids.PLParserParser.Fetch_stmtContext;
import org.apache.doris.nereids.PLParserParser.Open_stmtContext;
import org.apache.doris.hplsql.Var.Type;
import org.apache.doris.hplsql.exception.QueryException;
import org.apache.doris.hplsql.executor.Metadata;
import org.apache.doris.hplsql.executor.QueryExecutor;
import org.apache.doris.hplsql.executor.QueryResult;
import org.apache.doris.hplsql.objects.Table;

import org.antlr.v4.runtime.ParserRuleContext;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * HPL/SQL statements execution
 */
public class Stmt {
    Exec exec = null;
    Stack<Var> stack = null;
    Conf conf;
    Meta meta;
    Console console;

    boolean trace = false;
    private QueryExecutor queryExecutor;

    Stmt(Exec e, QueryExecutor queryExecutor) {
        exec = e;
        stack = exec.getStack();
        conf = exec.getConf();
        meta = exec.getMeta();
        trace = exec.getTrace();
        console = exec.console;
        this.queryExecutor = queryExecutor;
    }

    /**
     * ALLOCATE CURSOR statement
     */
    public Integer allocateCursor(Allocate_cursor_stmtContext ctx) {
        trace(ctx, "ALLOCATE CURSOR");
        String name = ctx.ident_pl(0).getText();
        Var cur = null;
        if (ctx.T_PROCEDURE() != null) {
            cur = exec.consumeReturnCursor(ctx.ident_pl(1).getText());
        } else if (ctx.T_RESULT() != null) {
            cur = exec.findVariable(ctx.ident_pl(1).getText());
            if (cur != null && cur.type != Type.RS_LOCATOR) {
                cur = null;
            }
        }
        if (cur == null) {
            trace(ctx, "Cursor for procedure not found: " + name);
            exec.signal(Signal.Type.SQLEXCEPTION);
            return -1;
        }
        exec.addVariable(new Var(name, Type.CURSOR, cur.value));
        return 0;
    }

    /**
     * ASSOCIATE LOCATOR statement
     */
    public Integer associateLocator(Associate_locator_stmtContext ctx) {
        trace(ctx, "ASSOCIATE LOCATOR");
        int cnt = ctx.ident_pl().size();
        if (cnt < 2) {
            return -1;
        }
        String procedure = ctx.ident_pl(cnt - 1).getText();
        for (int i = 0; i < cnt - 1; i++) {
            Var cur = exec.consumeReturnCursor(procedure);
            if (cur != null) {
                String name = ctx.ident_pl(i).getText();
                Var loc = exec.findVariable(name);
                if (loc == null) {
                    loc = new Var(name, Type.RS_LOCATOR, cur.value);
                    exec.addVariable(loc);
                } else {
                    loc.setValue(cur.value);
                }
            }
        }
        return 0;
    }

    /**
     * DECLARE cursor statement
     */
    public Integer declareCursor(Declare_cursor_itemContext ctx) {
        String name = ctx.ident_pl().getText();
        if (trace) {
            trace(ctx, "DECLARE CURSOR " + name);
        }
        Cursor cursor = new Cursor(null);
        if (ctx.expr() != null) {
            cursor.setExprCtx(ctx.expr());
        } else if (ctx.query() != null) {
            cursor.setSelectCtx(ctx.query());
        }
        if (ctx.cursor_with_return() != null) {
            cursor.setWithReturn(true);
        }
        Var var = new Var(name, Type.CURSOR, cursor);
        exec.addVariable(var);
        return 0;
    }

    /**
     * OPEN cursor statement
     */
    public Integer open(Open_stmtContext ctx) {
        trace(ctx, "OPEN");
        Cursor cursor = null;
        Var var = null;
        String cursorName = ctx.ident_pl().getText();
        String sql = null;
        if (ctx.T_FOR() != null) {                             // SELECT statement or dynamic SQL
            sql = ctx.expr() != null ? evalPop(ctx.expr()).toString() : evalPop(ctx.query()).toString();
            cursor = new Cursor(sql);
            var = exec.findCursor(cursorName);                      // Can be a ref cursor variable
            if (var == null) {
                var = new Var(cursorName, Type.CURSOR, cursor);
                exec.addVariable(var);
            } else {
                var.setValue(cursor);
            }
        } else {                                                 // Declared cursor
            var = exec.findVariable(cursorName);
            if (var != null && var.type == Type.CURSOR) {
                cursor = (Cursor) var.value;
                if (cursor.getSqlExpr() != null) {
                    sql = evalPop(cursor.getSqlExpr()).toString();
                    cursor.setSql(sql);
                } else if (cursor.getSqlSelect() != null) {
                    sql = evalPop(cursor.getSqlSelect()).toString();
                    cursor.setSql(sql);
                }
            }
        }
        if (cursor != null) {
            if (trace) {
                trace(ctx, cursorName + ": " + sql);
            }
            cursor.open(queryExecutor, ctx);
            QueryResult queryResult = cursor.getQueryResult();
            if (queryResult.error()) {
                exec.signal(queryResult);
                return 1;
            } else if (!exec.getOffline()) {
                exec.setSqlCode(SqlCodes.SUCCESS);
            }
            if (cursor.isWithReturn()) {
                exec.addReturnCursor(var);
            }
        } else {
            trace(ctx, "Cursor not found: " + cursorName);
            exec.setSqlCode(SqlCodes.ERROR);
            exec.signal(Signal.Type.SQLEXCEPTION);
            return 1;
        }
        return 0;
    }

    /**
     * FETCH cursor statement
     */
    public Integer fetch(Fetch_stmtContext ctx) {
        trace(ctx, "FETCH");
        String name = ctx.ident_pl(0).getText();
        Var varCursor = exec.findCursor(name);
        if (varCursor == null) {
            trace(ctx, "Cursor not found: " + name);
            exec.setSqlCode(SqlCodes.ERROR);
            exec.signal(Signal.Type.SQLEXCEPTION);
            return 1;
        } else if (varCursor.value == null) {
            trace(ctx, "Cursor not open: " + name);
            exec.setSqlCode(SqlCodes.ERROR);
            exec.signal(Signal.Type.SQLEXCEPTION);
            return 1;
        } else if (exec.getOffline()) {
            exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
            exec.signal(Signal.Type.NOTFOUND);
            return 0;
        }
        // Assign values from the row to local variables
        try {
            Cursor cursor = (Cursor) varCursor.value;
            int cols = ctx.ident_pl().size() - 1;
            QueryResult queryResult = cursor.getQueryResult();

            if (ctx.bulk_collect_clause() != null) {
                long limit = ctx.fetch_limit() != null ? evalPop(ctx.fetch_limit().expr()).longValue() : -1;
                long rowIndex = 1;
                List<Table> tables = exec.intoTables(ctx, intoVariableNames(ctx, cols));
                tables.forEach(Table::removeAll);
                while (queryResult.next()) {
                    cursor.setFetch(true);
                    for (int i = 0; i < cols; i++) {
                        Table table = tables.get(i);
                        table.populate(queryResult, rowIndex, i);
                    }
                    rowIndex++;
                    if (limit != -1 && rowIndex - 1 >= limit) {
                        break;
                    }
                }
            } else {
                if (queryResult.next()) {
                    cursor.setFetch(true);
                    for (int i = 0; i < cols; i++) {
                        Var var = exec.findVariable(ctx.ident_pl(i + 1).getText()); // FETCH into 后面的变量
                        if (var != null) { // 变量要提前DECLARE定义
                            if (var.type != Var.Type.ROW) {
                                var.setExpressionValue(queryResult, i); // 将每一列的值 set 到变量中
                            } else {
                                var.setRowValues(queryResult); // ？
                            }
                            if (trace) {
                                trace(ctx, var, queryResult.metadata(), i);
                            }
                        } else if (trace) {
                            trace(ctx, "Variable not found: " + ctx.ident_pl(i + 1).getText());
                        }
                    }
                    exec.incRowCount();
                    exec.setSqlSuccess();
                } else {
                    cursor.setFetch(false);
                    exec.setSqlCode(SqlCodes.NO_DATA_FOUND); // 退出
                }
            }
        } catch (QueryException e) {
            exec.setSqlCode(e);
            exec.signal(Signal.Type.SQLEXCEPTION, e.getMessage(), e);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    private List<String> intoVariableNames(Fetch_stmtContext ctx, int count) {
        return IntStream.range(0, count).mapToObj(i -> ctx.ident_pl(i + 1).getText()).collect(Collectors.toList());
    }


    /**
     * CLOSE cursor statement
     */
    public Integer close(Close_stmtContext ctx) {
        trace(ctx, "CLOSE");
        String name = ctx.L_ID().toString();
        Var var = exec.findVariable(name);
        if (var != null && var.type == Type.CURSOR) {
            ((Cursor) var.value).close();
            exec.setSqlCode(SqlCodes.SUCCESS);
        } else if (trace) {
            trace(ctx, "Cursor not found: " + name);
        }
        return 0;
    }

    /**
     * INCLUDE statement
     */
    public Integer include(Include_stmtContext ctx) {
        String file;
        if (ctx.file_name() != null) {
            file = ctx.file_name().getText();
        } else {
            file = evalPop(ctx.expr()).toString();
        }
        trace(ctx, "INCLUDE " + file);
        exec.includeFile(file, true);
        return 0;
    }

    /**
     * IF statement (PL/SQL syntax)
     */
    public Integer ifPlsql(If_plsql_stmtContext ctx) {
        boolean trueExecuted = false;
        trace(ctx, "IF");
        if (evalPop(ctx.bool_expr()).isTrue()) {
            trace(ctx, "IF TRUE executed");
            visit(ctx.block());
            trueExecuted = true;
        } else if (ctx.elseif_block() != null) {
            int cnt = ctx.elseif_block().size();
            for (int i = 0; i < cnt; i++) {
                if (evalPop(ctx.elseif_block(i).bool_expr()).isTrue()) {
                    trace(ctx, "ELSE IF executed");
                    visit(ctx.elseif_block(i).block());
                    trueExecuted = true;
                    break;
                }
            }
        }
        if (!trueExecuted && ctx.else_block() != null) {
            trace(ctx, "ELSE executed");
            visit(ctx.else_block());
        }
        return 0;
    }

    /**
     * IF statement (Transact-SQL syntax)
     */
    public Integer ifTsql(If_tsql_stmtContext ctx) {
        trace(ctx, "IF");
        visit(ctx.bool_expr());
        if (exec.stackPop().isTrue()) {
            trace(ctx, "IF TRUE executed");
            visit(ctx.single_block_stmt(0));
        } else if (ctx.T_ELSE() != null) {
            trace(ctx, "ELSE executed");
            visit(ctx.single_block_stmt(1));
        }
        return 0;
    }

    /**
     * IF statement (BTEQ syntax)
     */
    public Integer ifBteq(If_bteq_stmtContext ctx) {
        trace(ctx, "IF");
        visit(ctx.bool_expr());
        if (exec.stackPop().isTrue()) {
            trace(ctx, "IF TRUE executed");
            visit(ctx.single_block_stmt());
        }
        return 0;
    }

    /**
     * Assignment from SELECT statement
     */
    public Integer assignFromSelect(Assignment_stmt_select_itemContext ctx) {
        String sql = evalPop(ctx.select_stmt()).toString();
        if (trace) {
            trace(ctx, sql);
        }
        QueryResult query = queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        try {
            int cnt = ctx.ident_pl().size();
            if (query.next()) {
                for (int i = 0; i < cnt; i++) {
                    Var var = exec.findVariable(ctx.ident_pl(i).getText());
                    if (var != null) {
                        var.setValue(query, i);
                        if (trace) {
                            trace(ctx, "COLUMN: " + query.metadata().columnName(i) + ", " + query.metadata()
                                    .columnTypeName(i));
                            trace(ctx, "SET " + var.getName() + " = " + var.toString());
                        }
                    } else if (trace) {
                        trace(ctx, "Variable not found: " + ctx.ident_pl(i).getText());
                    }
                }
                exec.incRowCount();
                exec.setSqlSuccess();
            } else {
                exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
                exec.signal(Signal.Type.NOTFOUND);
            }
        } catch (QueryException e) {
            exec.signal(query);
            return 1;
        } finally {
            query.close();
        }
        return 0;
    }

    /**
     * SQL INSERT statement
     */
    public Integer insert(Insert_stmtContext ctx) {
        exec.stmtConnList.clear();
        if (ctx.select_stmt() != null) {
            return insertSelect(ctx);
        }
        return insertValues(ctx);
    }

    /**
     * SQL INSERT SELECT statement
     */
    public Integer insertSelect(Insert_stmtContext ctx) {
        trace(ctx, "INSERT SELECT");
        StringBuilder sql = new StringBuilder();
        sql.append(ctx.T_INSERT().getText() + " ");
        if (ctx.T_OVERWRITE() != null) {
            sql.append(ctx.T_OVERWRITE().getText() + " " + ctx.T_TABLE().getText() + " ");
        } else {
            sql.append(ctx.T_INTO().getText() + " ");
            if (ctx.T_TABLE() != null) {
                sql.append(ctx.T_TABLE().getText() + " ");
            }
        }
        sql.append(evalPop(ctx.table_name()).toString() + " ");
        sql.append(evalPop(ctx.select_stmt()).toString());
        trace(ctx, sql.toString());
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        query.close();
        return 0;
    }

    /**
     * SQL INSERT VALUES statement
     */
    public Integer insertValues(Insert_stmtContext ctx) {
        trace(ctx, "INSERT VALUES");
        String table = evalPop(ctx.table_name()).toString();
        String conn = exec.getObjectConnection(ctx.table_name().getText());
        Conn.Type type = exec.getConnectionType(conn);
        StringBuilder sql = new StringBuilder();
        if (type == Conn.Type.HIVE) {
            sql.append("INSERT INTO TABLE " + table + " ");
            if (conf.insertValues == Conf.InsertValues.NATIVE) {
                sql.append("VALUES\n(");
            }
        } else {
            sql.append("INSERT INTO " + table);
            if (ctx.insert_stmt_cols() != null) {
                sql.append(" " + exec.getFormattedText(ctx.insert_stmt_cols()));
            }
            sql.append(" VALUES\n(");
        }
        int rows = ctx.insert_stmt_rows().insert_stmt_row().size();
        for (int i = 0; i < rows; i++) {
            Insert_stmt_rowContext row = ctx.insert_stmt_rows().insert_stmt_row(i);
            int cols = row.expr().size();
            for (int j = 0; j < cols; j++) {
                String value = evalPop(row.expr(j)).toSqlString();
                if (j == 0 && type == Conn.Type.HIVE && conf.insertValues == Conf.InsertValues.SELECT) {
                    sql.append("SELECT ");
                }
                sql.append(value);
                if (j + 1 != cols) {
                    sql.append(", ");
                }
            }
            if (type != Conn.Type.HIVE || conf.insertValues == Conf.InsertValues.NATIVE) {
                if (i + 1 == rows) {
                    sql.append(")");
                } else {
                    sql.append("),\n(");
                }
            } else if (type == Conn.Type.HIVE && conf.insertValues == Conf.InsertValues.SELECT) {
                if (conf.dualTable != null) {
                    sql.append(" FROM " + conf.dualTable);
                }
                if (i + 1 < rows) {
                    sql.append("\nUNION ALL\n");
                }
            }
        }
        if (trace) {
            trace(ctx, sql.toString());
        }

        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        query.close();
        return 0;
    }

    /**
     * INSERT DIRECTORY statement
     */
    public Integer insertDirectory(Insert_directory_stmtContext ctx) {
        trace(ctx, "INSERT DIRECTORY");
        StringBuilder sql = new StringBuilder();
        sql.append(ctx.T_INSERT().getText() + " " + ctx.T_OVERWRITE().getText() + " ");
        if (ctx.T_LOCAL() != null) {
            sql.append(ctx.T_LOCAL().getText() + " ");
        }
        sql.append(ctx.T_DIRECTORY().getText() + " " + evalPop(ctx.expr_file()).toSqlString() + " ");
        sql.append(evalPop(ctx.expr_select()).toString());
        trace(ctx, sql.toString());
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        query.close();
        return 0;
    }

    /**
     * GET DIAGNOSTICS EXCEPTION statement
     */
    public Integer getDiagnosticsException(
            Get_diag_stmt_exception_itemContext ctx) {
        trace(ctx, "GET DIAGNOSTICS EXCEPTION");
        Signal signal = exec.signalPeek();
        if (signal == null || (signal != null && signal.type != Signal.Type.SQLEXCEPTION)) {
            signal = exec.currentSignal;
        }
        if (signal != null) {
            exec.setVariable(ctx.qident().getText(), signal.getValue());
        }
        return 0;
    }

    /**
     * GET DIAGNOSTICS ROW_COUNT statement
     */
    public Integer getDiagnosticsRowCount(Get_diag_stmt_rowcount_itemContext ctx) {
        trace(ctx, "GET DIAGNOSTICS ROW_COUNT");
        exec.setVariable(ctx.qident().getText(), exec.getRowCount());
        return 0;
    }

    /**
     * USE statement
     */
    public Integer use(Use_stmtContext ctx) {
        trace(ctx, "USE");
        return use(ctx, ctx.T_USE().toString() + " " + meta.normalizeIdentifierPart(ctx.expr().getText()));
    }

    public Integer use(ParserRuleContext ctx, String sql) {
        if (trace) {
            trace(ctx, "SQL statement: " + sql);
        }
        QueryResult query = queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlCode(SqlCodes.SUCCESS);
        query.close();
        return 0;
    }

    /**
     * VALUES statement
     */
    public Integer values(Values_into_stmtContext ctx) {
        trace(ctx, "VALUES statement");
        int cnt = ctx.ident_pl().size();        // Number of variables and assignment expressions
        int ecnt = ctx.expr().size();
        for (int i = 0; i < cnt; i++) {
            String name = ctx.ident_pl(i).getText();
            if (i < ecnt) {
                visit(ctx.expr(i));
                Var var = exec.setVariable(name);
                if (trace) {
                    trace(ctx, "SET " + name + " = " + var.toString());
                }
            }
        }
        return 0;
    }

    /**
     * WHILE statement
     */
    public Integer while_(While_stmtContext ctx) {
        trace(ctx, "WHILE - ENTERED");
        String label = exec.labelPop();
        while (true) {
            if (evalPop(ctx.bool_expr()).isTrue()) {
                exec.enterScope(Scope.Type.LOOP);
                visit(ctx.block());
                exec.leaveScope();
                if (canContinue(label)) {
                    continue;
                }
            }
            break;
        }
        trace(ctx, "WHILE - LEFT");
        return 0;
    }

    /**
     * FOR cursor statement
     */
    public Integer forCursor(For_cursor_stmtContext ctx) {
        trace(ctx, "FOR CURSOR - ENTERED");
        exec.enterScope(Scope.Type.LOOP);
        String cursor = ctx.L_ID().getText();
        String sql = evalPop(ctx.select_stmt()).toString();
        trace(ctx, sql);
        QueryResult query = exec.queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        trace(ctx, "SELECT completed successfully");
        exec.setSqlSuccess();
        try {
            int cols = query.columnCount();
            Row row = new Row();
            for (int i = 0; i < cols; i++) {
                row.addColumnDefinition(query.metadata().columnName(i), query.metadata().columnTypeName(i));
            }
            Var var = new Var(cursor, row);
            exec.addVariable(var);
            while (query.next()) {
                var.setRowValues(query);
                if (trace) {
                    trace(ctx, var, query.metadata(), 0);
                }
                visit(ctx.block());
                exec.incRowCount();
            }
        } catch (QueryException e) {
            exec.signal(e);
            query.close();
            return 1;
        }
        exec.setSqlSuccess();
        query.close();
        exec.leaveScope();
        trace(ctx, "FOR CURSOR - LEFT");
        return 0;
    }

    /**
     * FOR (integer range) statement
     */
    public Integer forRange(For_range_stmtContext ctx) {
        trace(ctx, "FOR RANGE - ENTERED");
        int start = evalPop(ctx.expr(0)).intValue();
        int end = evalPop(ctx.expr(1)).intValue();
        int step = evalPop(ctx.expr(2), 1L).intValue();
        exec.enterScope(Scope.Type.LOOP);
        Var index = setIndex(start, end, ctx);
        exec.addVariable(index);
        for (int i = start; i <= end; i += step) {
            visit(ctx.block());
            updateIndex(step, index, ctx);
        }
        exec.leaveScope();
        trace(ctx, "FOR RANGE - LEFT");
        return 0;
    }

    public Integer unconditionalLoop(Unconditional_loop_stmtContext ctx) {
        trace(ctx, "UNCONDITIONAL LOOP - ENTERED");
        String label = exec.labelPop();
        while (true) {
            exec.enterScope(Scope.Type.LOOP);
            visit(ctx.block());
            exec.leaveScope();
            if (!canContinue(label)) {
                break;
            }
        }
        trace(ctx, "UNCONDITIONAL LOOP - LEFT");
        return 0;
    }

    /**
     * To set the Value index for FOR Statement
     */
    private Var setIndex(int start, int end, For_range_stmtContext ctx) {

        if (ctx.T_REVERSE() == null) {
            return new Var(ctx.L_ID().getText(), Long.valueOf(start));
        } else {
            return new Var(ctx.L_ID().getText(), Long.valueOf(end));
        }
    }

    /**
     * To update the value of index for FOR Statement
     */
    private void updateIndex(int step, Var index, For_range_stmtContext ctx) {

        if (ctx.T_REVERSE() == null) {
            index.increment(step);
        } else {
            index.decrement(step);
        }
    }

    /**
     * EXEC, EXECUTE and EXECUTE IMMEDIATE statement to execute dynamic SQL or stored procedure
     */
    public Integer exec(Exec_stmtContext ctx) {
        if (execProc(ctx)) {
            return 0;
        }
        trace(ctx, "EXECUTE");
        Var vsql = evalPop(ctx.expr());
        String sql = vsql.toString();
        if (trace) {
            trace(ctx, "SQL statement: " + sql);
        }
        QueryResult query = queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        try {
            if (ctx.T_INTO() != null) {
                int cols = ctx.L_ID().size();
                if (query.next()) {
                    for (int i = 0; i < cols; i++) {
                        Var var = exec.findVariable(ctx.L_ID(i).getText());
                        if (var != null) {
                            if (var.type != Type.ROW) {
                                var.setValue(query, i);
                            } else {
                                var.setRowValues(query);
                            }
                            if (trace) {
                                trace(ctx, var, query.metadata(), i);
                            }
                        } else if (trace) {
                            trace(ctx, "Variable not found: " + ctx.L_ID(i).getText());
                        }
                    }
                    exec.setSqlCode(SqlCodes.SUCCESS);
                }
            } else { // Print the results
                int cols = query.columnCount();
                while (query.next()) {
                    for (int i = 0; i < cols; i++) {
                        if (i > 1) {
                            console.print("\t");
                        }
                        console.print(query.column(i, String.class));
                    }
                    console.printLine("");
                }
            }
        } catch (QueryException e) {
            exec.signal(query);
            query.close();
            return 1;
        }
        query.close();
        return 0;
    }

    /**
     * EXEC to execute a stored procedure
     */
    public Boolean execProc(Exec_stmtContext ctx) {
        String name = evalPop(ctx.expr()).toString().toUpperCase();
        if (exec.functions.exec(name, ctx.expr_func_params())) {
            return true;
        }
        return false;
    }

    /**
     * EXIT statement (leave the specified loop with a condition)
     */
    public Integer exit(Exit_stmtContext ctx) {
        trace(ctx, "EXIT");
        String label = "";
        if (ctx.L_ID() != null) {
            label = ctx.L_ID().toString();
        }
        if (ctx.T_WHEN() != null) {
            if (evalPop(ctx.bool_expr()).isTrue()) {
                leaveLoop(label);
            }
        } else {
            leaveLoop(label);
        }
        return 0;
    }

    /**
     * BREAK statement (leave the innermost loop unconditionally)
     */
    public Integer break_(Break_stmtContext ctx) {
        trace(ctx, "BREAK");
        leaveLoop("");
        return 0;
    }

    /**
     * LEAVE statement (leave the specified loop unconditionally)
     */
    public Integer leave(Leave_stmtContext ctx) {
        trace(ctx, "LEAVE");
        String label = "";
        if (ctx.L_ID() != null) {
            label = ctx.L_ID().toString();
        }
        leaveLoop(label);
        return 0;
    }

    /**
     * Leave the specified or innermost loop unconditionally
     */
    public void leaveLoop(String value) {
        exec.signal(Signal.Type.LEAVE_LOOP, value);
    }

    /**
     * UPDATE statement
     */
    public Integer update(Update_stmtContext ctx) {
        trace(ctx, "UPDATE");
        String sql = exec.getFormattedText(ctx);
        trace(ctx, sql);
        QueryResult query = queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        query.close();
        return 0;
    }

    /**
     * DELETE statement
     */
    public Integer delete(Delete_stmtContext ctx) {
        trace(ctx, "DELETE");
        String table = evalPop(ctx.table_name()).toString();
        StringBuilder sql = new StringBuilder();
        if (ctx.T_ALL() == null) {
            sql.append("DELETE FROM " + table);
            if (ctx.where_clause() != null) {
                boolean oldBuildSql = exec.buildSql;
                exec.buildSql = true;
                sql.append(" " + evalPop(ctx.where_clause()).toString());
                exec.buildSql = oldBuildSql;
            }
        } else {
            sql.append("TRUNCATE TABLE " + table);
        }
        trace(ctx, sql.toString());
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        query.close();
        return 0;
    }

    /**
     * MERGE statement
     */
    public Integer merge(Merge_stmtContext ctx) {
        trace(ctx, "MERGE");
        String sql = exec.getFormattedText(ctx);
        trace(ctx, sql);
        QueryResult query = queryExecutor.executeQuery(sql, ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        query.close();
        return 0;
    }

    /**
     * PRINT Statement
     */
    public Integer print(Print_stmtContext ctx) {
        trace(ctx, "PRINT");
        if (ctx.expr() != null) {
            console.printLine(evalPop(ctx.expr()).toString());
        }
        return 0;
    }

    /**
     * QUIT Statement
     */
    public Integer quit(Quit_stmtContext ctx) {
        trace(ctx, "QUIT");
        String rc = null;
        if (ctx.expr() != null) {
            rc = evalPop(ctx.expr()).toString();
        }
        exec.signal(Signal.Type.LEAVE_PROGRAM, rc);
        return 0;
    }

    /**
     * SET current schema
     */
    public Integer setCurrentSchema(Set_current_schema_optionContext ctx) {
        trace(ctx, "SET CURRENT SCHEMA");
        return use(ctx, "USE " + meta.normalizeIdentifierPart(evalPop(ctx.expr()).toString()));
    }

    /**
     * SIGNAL statement
     */
    public Integer signal(Signal_stmtContext ctx) {
        trace(ctx, "SIGNAL");
        Signal signal = new Signal(Signal.Type.USERDEFINED, ctx.ident_pl().getText());
        exec.signal(signal);
        return 0;
    }

    /**
     * SUMMARY statement
     */
    public Integer summary(Summary_stmtContext ctx) {
        trace(ctx, "SUMMARY");
        String table = null;
        String select = null;
        String conn = null;
        Row row = null;
        if (ctx.table_name() != null) {
            table = evalPop(ctx.table_name()).toString();
            conn = exec.getObjectConnection(table);
            row = meta.getRowDataType(ctx, conn, table);
        } else {
            select = evalPop(ctx.select_stmt()).toString();
            conn = exec.getStatementConnection();
            row = meta.getRowDataTypeForSelect(ctx, conn, select);
        }
        if (row == null) {
            return 1;
        }
        Conn.Type connType = exec.getConnectionType(conn);
        if (ctx.T_TOP() == null) {
            return summaryStat(ctx, table, select, row, conn, connType);
        } else {
            return summaryTop(ctx, table, select, row, conn, connType);
        }
    }

    // Summary for column statistics
    public Integer summaryStat(Summary_stmtContext ctx, String table,
            String select, Row row, String conn, Conn.Type connType) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*)");
        int maxColName = 11;
        // Define summary metrics for each column
        for (Column c : row.getColumns()) {
            String col = c.getName();
            if (connType == Conn.Type.HIVE) {
                col = '`' + col + '`';
            }
            sql.append(",COUNT(" + col + "),");
            sql.append("COUNT(DISTINCT " + col + "),");
            sql.append("AVG(" + col + "),");
            sql.append("MIN(" + col + "),");
            sql.append("MAX(" + col + "),");
            sql.append("STDDEV_SAMP(" + col + "),");
            sql.append("PERCENTILE_APPROX(CAST(" + col + " AS DOUBLE),0.05),");
            sql.append("PERCENTILE_APPROX(CAST(" + col + " AS DOUBLE),0.25),");
            sql.append("PERCENTILE_APPROX(CAST(" + col + " AS DOUBLE),0.5),");
            sql.append("PERCENTILE_APPROX(CAST(" + col + " AS DOUBLE),0.75),");
            sql.append("PERCENTILE_APPROX(CAST(" + col + " AS DOUBLE),0.95)");
            if (col.length() > maxColName) {
                maxColName = col.length();
            }
        }
        if (table != null) {
            sql.append(" FROM (SELECT * FROM " + table);
            if (ctx.where_clause() != null) {
                sql.append(" " + evalPop(ctx.where_clause()).toString());
            }
            if (ctx.T_LIMIT() != null) {
                sql.append(" LIMIT ");
                int limExp = 0;
                if (ctx.T_TOP() != null) {
                    limExp = 1;
                }
                sql.append(evalPop(ctx.expr(limExp)).toString());
            }
            sql.append(") t");
        } else {
            sql.append(" FROM (" + select + ") t");
        }
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        try {
            System.out.print("\n");
            // The summary query returns only one row
            if (query.next()) {
                int i = 0;
                int cc = 11;
                String cntRows = query.column(0, String.class);
                // Pad output
                String fmt = String.format("%%-%ds\t%%-11s\t%%-11s\t%%-11s\t%%-11s\t%%-11s\t%%-11s\t%%-11s\t%%-11s"
                        + "\t%%-11s\t%%-11s\t%%-11s\t%%-11s\t%%-11s\n", maxColName + 1);
                System.out.print(
                        String.format(fmt, "Column", "Type", "Rows", "NonNull", "Unique", "Avg", "Min", "Max", "StdDev",
                                "p05", "p25", "p50", "p75", "p95"));
                for (Column c : row.getColumns()) {
                    String avg = String.format("%.2f", query.column(3 + i * cc, Double.class));
                    String stddev = String.format("%.2f", query.column(6 + i * cc, Double.class));
                    String p05 = String.format("%.2f", query.column(7 + i * cc, Double.class));
                    String p25 = String.format("%.2f", query.column(8 + i * cc, Double.class));
                    String p50 = String.format("%.2f", query.column(9 + i * cc, Double.class));
                    String p75 = String.format("%.2f", query.column(10 + i * cc, Double.class));
                    String p95 = String.format("%.2f", query.column(11 + i * cc, Double.class));
                    System.out.print(String.format(fmt, c.getName(), c.getType(), cntRows,
                            query.column(1 + i * cc, String.class), query.column(2 + i * cc, String.class), avg,
                            query.column(4 + i * cc, String.class), query.column(5 + i * cc, String.class), stddev, p05,
                            p25, p50, p75, p95));
                    i++;
                }
            }
        } catch (QueryException e) {
            exec.signal(e);
            query.close();
            return 1;
        }
        query.close();
        return 0;
    }

    // Summary for top column values
    public Integer summaryTop(Summary_stmtContext ctx, String table, String select,
            Row row, String conn, Conn.Type connType) {
        // CAST AS INT does not work as expected (ID is still considered as STRING in ORDER BY for some reason)
        StringBuilder sql = new StringBuilder("SELECT id, col, cnt FROM ("
                + "SELECT id, col, cnt, ROW_NUMBER() OVER (PARTITION BY id ORDER BY cnt DESC) rn "
                + "FROM (SELECT CAST(GROUPING__ID AS DECIMAL) id, COALESCE(");
        int topNum = evalPop(ctx.expr(0)).intValue();
        StringBuilder colsList = new StringBuilder();
        StringBuilder colsGrList = new StringBuilder();
        int i = 0;
        for (Column c : row.getColumns()) {
            String col = c.getName();
            if (connType == Conn.Type.HIVE) {
                col = '`' + col + '`';
            }
            if (i != 0) {
                colsList.append(",");
                colsGrList.append(",");
            }
            colsList.append(col);
            colsGrList.append("(" + col + ")");
            i++;
        }
        sql.append(colsList);
        sql.append(") col, COUNT(*) cnt");
        if (table != null) {
            sql.append(" FROM (SELECT * FROM " + table);
            if (ctx.where_clause() != null) {
                sql.append(" " + evalPop(ctx.where_clause()).toString());
            }
            if (ctx.T_LIMIT() != null) {
                sql.append(" LIMIT " + evalPop(ctx.expr(1)).toString());
            }
            sql.append(") t");
        } else {
            sql.append(" FROM (" + select + ") t");
        }
        sql.append(" GROUP BY ");
        sql.append(colsList);
        sql.append(" GROUPING SETS (");
        sql.append(colsGrList);
        sql.append(")) t) t WHERE rn <= " + topNum + " ORDER BY id, cnt DESC");
        // Add LIMIT as Order by-s without limit can disabled for safety reasons
        sql.append(" LIMIT " + topNum * row.size());
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        try {
            int prevId = -1;
            int grRow = 0;
            int colNum = 0;
            int maxLen = row.getColumn(colNum).getName().length();
            ArrayList<String> outCols = new ArrayList<>();
            ArrayList<Integer> outCnts = new ArrayList<>();
            ArrayList<Integer> outLens = new ArrayList<>();
            while (query.next()) {
                int id = query.column(0, Integer.class);
                String value = query.column(1, String.class);
                int cnt = query.column(2, Integer.class);
                if (prevId == -1) {
                    prevId = id;
                }
                // Still the same column
                if (id == prevId) {
                    outCols.add(value);
                    outCnts.add(cnt);
                    if (value != null && value.length() > maxLen) {
                        maxLen = Math.min(value.length(), 300);
                    }
                    grRow++;
                } else { // First value for next column
                    // Pad with empty rows if the number of values in group is less than TOP num
                    for (int j = grRow; j < topNum; j++) {
                        outCols.add("");
                        outCnts.add(0);
                        grRow++;
                    }
                    outCols.add(value);
                    outCnts.add(cnt);
                    outLens.add(maxLen);
                    colNum++;
                    maxLen = row.getColumn(colNum).getName().length();
                    if (value != null && value.length() > maxLen) {
                        maxLen = Math.min(value.length(), 300);
                    }
                    grRow = 1;
                    prevId = id;
                }
            }
            for (int j = grRow; j < topNum; j++) {
                outCols.add("");
                outCnts.add(0);
                grRow++;
            }
            if (maxLen != 0) {
                outLens.add(maxLen);
            }
            System.out.print("\n");
            // Output header
            i = 0;
            for (Column c : row.getColumns()) {
                if (i != 0) {
                    System.out.print("\t");
                }
                String fmt = String.format("%%-%ds", outLens.get(i) + 11 + 3);
                System.out.print(String.format(fmt, c.getName()));
                i++;
            }
            System.out.print("\n");
            // Output top values
            for (int j = 0; j < topNum; j++) {
                for (int k = 0; k < row.size(); k++) {
                    if (k != 0) {
                        System.out.print("\t");
                    }
                    int cnt = outCnts.get(j + k * topNum);
                    if (cnt != 0) { // skip padded values
                        String fmt = String.format("%%-%ds", outLens.get(k));
                        System.out.print(String.format(fmt, outCols.get(j + k * topNum)));
                        System.out.print(String.format("   %-11d", cnt));
                    } else {
                        String fmt = String.format("%%-%ds", outLens.get(k) + 11 + 3);
                        System.out.print(String.format(fmt, ""));
                    }
                }
                System.out.print("\n");
            }
        } catch (QueryException e) {
            exec.signal(e);
            query.close();
            return 1;
        }
        query.close();
        return 0;
    }

    /**
     * RESIGNAL statement
     */
    public Integer resignal(Resignal_stmtContext ctx) {
        trace(ctx, "RESIGNAL");
        if (ctx.T_SQLSTATE() != null) {
            String sqlstate = evalPop(ctx.expr(0)).toString();
            String text = "";
            if (ctx.T_MESSAGE_TEXT() != null) {
                text = evalPop(ctx.expr(1)).toString();
            }
            SQLException exception = new SQLException(text, sqlstate, -1);
            Signal signal = new Signal(Signal.Type.SQLEXCEPTION, text, exception);
            exec.setSqlCode(exception);
            exec.resignal(signal);
        } else {
            exec.resignal();
        }
        return 0;
    }

    /**
     * RETURN statement
     */
    public Integer return_(Return_stmtContext ctx) {
        trace(ctx, "RETURN");
        if (ctx.expr() != null) {
            eval(ctx.expr());
        }
        exec.signal(Signal.Type.LEAVE_ROUTINE);
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
            }
            return false;
        }
        return true;
    }

    /**
     * Evaluate the expression and push the value to the stack
     */
    void eval(ParserRuleContext ctx) {
        exec.visit(ctx);
    }

    /**
     * Evaluate the expression to specified String value
     */
    void evalString(String string) {
        exec.stackPush(new Var(string));
    }

    void evalString(StringBuilder string) {
        evalString(string.toString());
    }

    /**
     * Evaluate the expression and pop value from the stack
     */
    Var evalPop(ParserRuleContext ctx) {
        visit(ctx);
        if (!exec.stack.isEmpty()) {
            return exec.stackPop();
        }
        return Var.Empty;
    }

    Var evalPop(ParserRuleContext ctx, long def) {
        if (ctx != null) {
            exec.visit(ctx);
            return exec.stackPop();
        }
        return new Var(def);
    }

    /**
     * Execute rules
     */
    Integer visit(ParserRuleContext ctx) {
        return exec.visit(ctx);
    }

    /**
     * Execute children rules
     */
    Integer visitChildren(ParserRuleContext ctx) {
        return exec.visitChildren(ctx);
    }

    /**
     * Trace information
     */
    void trace(ParserRuleContext ctx, String message) {
        exec.trace(ctx, message);
    }

    void trace(ParserRuleContext ctx, Var var, Metadata metadata, int idx) {
        exec.trace(ctx, var, metadata, idx);
    }
}
