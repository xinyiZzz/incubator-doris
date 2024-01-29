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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Select.java
// and modified by Doris

package org.apache.doris.hplsql;

import org.apache.doris.nereids.DorisParser.FromClauseContext;
import org.apache.doris.nereids.DorisParser.QueryContext;
import org.apache.doris.nereids.DorisParser.RegularQuerySpecificationContext;
import org.apache.doris.nereids.DorisParser.RelationContext;
import org.apache.doris.nereids.DorisParser.SelectClauseContext;
import org.apache.doris.nereids.DorisParser.SelectColumnClauseContext;
import org.apache.doris.nereids.DorisParser.StatementContext;
import org.apache.doris.nereids.DorisParser.StatementDefaultContext;
import org.apache.doris.nereids.DorisParser.WhereClauseContext;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.hplsql.exception.QueryException;
import org.apache.doris.hplsql.exception.TypeException;
import org.apache.doris.hplsql.exception.UndefinedIdentException;
import org.apache.doris.hplsql.executor.HplsqlResult;
import org.apache.doris.hplsql.executor.QueryExecutor;
import org.apache.doris.hplsql.executor.QueryResult;
import org.apache.doris.hplsql.executor.ResultListener;
import org.apache.doris.hplsql.objects.Table;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Select {
    Exec exec = null;
    Stack<Var> stack = null;
    Conf conf;
    Console console;
    ResultListener resultListener = ResultListener.NONE;
    QueryExecutor queryExecutor;

    boolean trace = false;

    Select(Exec e, QueryExecutor queryExecutor) {
        this.exec = e;
        this.stack = exec.getStack();
        this.conf = exec.getConf();
        this.trace = exec.getTrace();
        this.console = exec.console;
        this.queryExecutor = queryExecutor;
    }

    public void setResultListener(ResultListener resultListener) {
        this.resultListener = resultListener;
    }

    /**
     * Executing or building SELECT statement
     */
    public Integer select(StatementDefaultContext ctx) {
        if (ctx.parent instanceof StatementContext) {
            exec.stmtConnList.clear();
            trace(ctx, "SELECT");
        }
        boolean oldBuildSql = exec.buildSql;
        exec.buildSql = true;
        StringBuilder sql = new StringBuilder();
        visitStatementDefault(ctx);
        sql.append(exec.stackPop());
        exec.buildSql = oldBuildSql;
        // No need to execute at this stage
        if (!(ctx.parent instanceof StatementContext)) {
            exec.stackPush(sql);
            return 0;
        }
        if (trace) {
            trace(ctx, sql.toString());
        }
        if (exec.getOffline()) {
            trace(ctx, "Not executed - offline mode set");
            return 0;
        }

        // QueryResult query = queryExecutor.executeQuery(exec.logicalPlanBuilder.getOriginSql(ctx.statementDefault()),
        //         ctx);
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        resultListener.setProcessor(query.processor());
        ConnectContext preConnectContext;
        if (query.processor() != null) {
            preConnectContext = query.processor().getCtx();
        } else {
            preConnectContext = ConnectContext.get();
        }

        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        trace(ctx, "SELECT completed successfully");
        exec.setSqlSuccess();
        try (AutoCloseConnectContext autoCloseCtx = new AutoCloseConnectContext(preConnectContext)) {
            autoCloseCtx.call();
            // int intoCount = getIntoCount(ctx);
            // if (intoCount > 0) {
            //     if (isBulkCollect(ctx)) {
            //         trace(ctx, "SELECT BULK COLLECT INTO statement executed");
            //         long rowIndex = 1;
            //         List<Table> tables = exec.intoTables(ctx, intoVariableNames(ctx, intoCount));
            //         tables.forEach(Table::removeAll);
            //         while (query.next()) {
            //             for (int i = 0; i < intoCount; i++) {
            //                 Table table = tables.get(i);
            //                 table.populate(query, rowIndex, i);
            //             }
            //             rowIndex++;
            //         }
            //     } else {
            //         trace(ctx, "SELECT INTO statement executed");
            //         if (query.next()) {
            //             for (int i = 0; i < intoCount; i++) {
            //                 populateVariable(ctx, query, i);
            //             }
            //             exec.incRowCount();
            //             exec.setSqlSuccess();
            //             if (query.next()) {
            //                 exec.setSqlCode(SqlCodes.TOO_MANY_ROWS);
            //                 exec.signal(Signal.Type.TOO_MANY_ROWS);
            //             }
            //         } else {
            //             exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
            //             exec.signal(Signal.Type.NOTFOUND);
            //         }
            //     }
            // } else
            if (ctx.parent instanceof StatementContext) {
                // Print all results for standalone SELECT statement
                resultListener.onMetadata(query.metadata());
                int cols = query.columnCount();
                if (trace) {
                    trace(ctx, "Standalone SELECT executed: " + cols + " columns in the result set");
                }
                while (query.next()) {
                    if (resultListener instanceof HplsqlResult) { //hplsql.sh 不会走，mysql client中set hpl=true会走
                        resultListener.onMysqlRow(query.mysqlRow());
                    } else { // hplsql 什么时候走到这, hplsql.sh
                        Object[] row = new Object[cols]; // hplsql，这数据量要是很大岂不炸了
                        for (int i = 0; i < cols; i++) {
                            row[i] = query.column(i, Object.class);
                            if (i > 0) {
                                console.print("\t");
                            }
                            console.print(String.valueOf(row[i]));
                        }
                        console.printLine("");
                        exec.incRowCount();

                        resultListener.onRow(row);
                    }
                }
                resultListener.onEof();
            } else { // Scalar subquery
                trace(ctx, "Scalar subquery executed, first row and first column fetched only");
                if (query.next()) {
                    exec.stackPush(new Var().setValue(query, 1));
                    exec.setSqlSuccess();
                } else {
                    evalNull();
                    exec.setSqlCode(SqlCodes.NO_DATA_FOUND);
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
     * Part of SELECT
     */
    public Integer visitStatementDefault(StatementDefaultContext ctx) {
        // int cnt = ctx.fullselect_stmt_item().size();
        StringBuilder sql = new StringBuilder();
        // for (int i = 0; i < cnt; i++) {
        String part = evalPop(ctx.query()).toString();
        sql.append(part);
        // if (i + 1 != cnt) {
        //     sql.append("\n").append(getText(ctx.fullselect_set_clause(i))).append("\n");
        // }
        // }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * Part of SELECT
     */
    public Integer visitQuery(QueryContext ctx) {
        StringBuilder sql = new StringBuilder();
        String part = evalPop(ctx.queryTerm()).toString();
        sql.append(part);
        exec.stackPush(sql);
        return 0;
    }

    public Integer visitRegularQuerySpecification(RegularQuerySpecificationContext ctx) {
        // SelectClauseContext selectCtx = ctx.selectClause();
        // relation = visitFromClause(ctx.fromClause());
        // selectPlan = withSelectQuerySpecification(
        //         ctx, relation,
        //         selectCtx,
        //         Optional.ofNullable(ctx.whereClause()),
        //         Optional.ofNullable(ctx.aggClause()),
        //         Optional.ofNullable(ctx.havingClause()));
        // selectPlan = withQueryOrganization(selectPlan, ctx.queryOrganization());
        // return withSelectHint(selectPlan, selectCtx.selectHint());

        StringBuilder sql = new StringBuilder();
        sql.append(ctx.selectClause().SELECT());
        selectList(ctx.selectClause());
        exec.append(sql, exec.stackPop().toString(), ctx.selectClause().getStart(),
                ctx.selectClause().selectColumnClause().getStart());
        Token last = ctx.selectClause().selectColumnClause().stop;
        // if (ctx.into_clause() != null) {
        //     last = ctx.into_clause().stop;
        // }
        if (ctx.fromClause() != null) {
            exec.append(sql, evalPop(ctx.fromClause()).toString(), last, ctx.fromClause().getStart());
            last = ctx.fromClause().stop;
            // } else if (conf.dualTable != null) {
            //     sql.append(" FROM ").append(conf.dualTable);
            // relation = withOneRowRelation(columnCtx);
        }
        if (ctx.whereClause() != null) {
            sql.append(ctx.whereClause().WHERE());
            where(ctx.whereClause());
            exec.append(sql, exec.stackPop().toString(), last, ctx.whereClause().getStart());
            // last = ctx.whereClause().stop;
        }
        // if (ctx.group_by_clause() != null) {
        //     exec.append(sql, getText(ctx.group_by_clause()), last, ctx.group_by_clause().getStart());
        //     last = ctx.group_by_clause().stop;
        // }
        // if (ctx.having_clause() != null) {
        //     exec.append(sql, getText(ctx.having_clause()), last, ctx.having_clause().getStart());
        //     last = ctx.having_clause().stop;
        // }
        // if (ctx.qualify_clause() != null) {
        //     exec.append(sql, getText(ctx.qualify_clause()), last, ctx.qualify_clause().getStart());
        //     last = ctx.qualify_clause().stop;
        // }
        // if (ctx.order_by_clause() != null) {
        //     exec.append(sql, getText(ctx.order_by_clause()), last, ctx.order_by_clause().getStart());
        //     last = ctx.order_by_clause().stop;
        // }
        // if (ctx.select_options() != null) {
        //     Var opt = evalPop(ctx.select_options());
        //     if (!opt.isNull()) {
        //         sql.append(" " + opt.toString());
        //     }
        // }
        // if (ctx.select_list().select_list_limit() != null) {
        //     sql.append(" LIMIT " + evalPop(ctx.select_list().select_list_limit().expr()));
        // }
        exec.stackPush(sql);
        return 0;
    }

    private void populateVariable(Select_stmtContext ctx, QueryResult query,
            int columnIndex) {
        String intoName = getIntoVariable(ctx, columnIndex);
        Var var = exec.findVariable(intoName);
        if (var != null) {
            if (var.type == Var.Type.HPL_OBJECT && var.value instanceof Table) {
                Table table = (Table) var.value;
                table.populate(query, getIntoTableIndex(ctx, columnIndex), columnIndex);
            } else if (var.type == Var.Type.ROW) {
                var.setRowValues(query);
            } else {
                var.setValue(query, columnIndex);
            }
            exec.trace(ctx, var, query.metadata(), columnIndex);
        } else {
            throw new UndefinedIdentException(ctx, intoName);
        }
    }

    /**
     * SELECT list
     */
    public Integer selectList(SelectClauseContext ctx) {
        StringBuilder sql = new StringBuilder();
        SelectColumnClauseContext selectColumnCtx = ctx.selectColumnClause();
        List<NamedExpression> namedExpressions = exec.logicalPlanBuilder.getNamedExpressions(
                selectColumnCtx.namedExpressionSeq());
        int cnt = namedExpressions.size();
        for (int i = 0; i < cnt; i++) {
            sql.append(namedExpressions.get(i).toSql());
            if (i + 1 < cnt) {
                sql.append(", ");
            }
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * FROM clause
     */
    public Integer from(FromClauseContext ctx) {
        // 不能直接 ctx.getText() 会丢失空格
        StringBuilder sql = new StringBuilder();
        sql.append(ctx.FROM().getText()).append(" ");
        // sql.append(evalPop(ctx.from_table_clause()));
        if (ctx.relation() == null) {
            //
        }
        // LogicalPlan left = inputPlan;
        for (RelationContext relation : ctx.relation()) {
            // // build left deep join tree
            // LogicalPlan right = withJoinRelations(visitRelation(relation), relation);
            // left = (left == null) ? right :
            //         new LogicalJoin<>(
            //                 JoinType.CROSS_JOIN,
            //                 ExpressionUtils.EMPTY_CONDITION,
            //                 ExpressionUtils.EMPTY_CONDITION,
            //                 JoinHint.NONE,
            //                 Optional.empty(),
            //                 left,
            //                 right);
            // TODO: pivot and lateral view
            visitRelation(relation);
            sql.append(exec.stackPop());
        }
        exec.stackPush(sql);
        return 0;
    }

    /**
     * FROM Relation
     */
    public Integer visitRelation(RelationContext ctx) {
        StringBuilder sql = new StringBuilder();
        // sql.append(evalPop(ctx.relationPrimary()));
        sql.append(ctx.relationPrimary().getText()).append(" ");
        exec.stackPush(sql);
        return 0;
    }

    /**
     * WHERE clause
     */
    public Integer where(WhereClauseContext ctx) {
        StringBuilder sql = new StringBuilder();
        sql.append(exec.logicalPlanBuilder.getExpression(ctx.booleanExpression()).toSql());
        exec.stackPush(sql);
        return 0;
    }

    /**
     * Evaluate the expression to NULL
     */
    void evalNull() {
        exec.stackPush(Var.Null);
    }

    /**
     * Evaluate the expression and pop value from the stack
     */
    Var evalPop(ParserRuleContext ctx) {
        exec.visit(ctx);
        if (!exec.stack.isEmpty()) {
            return exec.stackPop();
        }
        return Var.Empty;
    }

    /**
     * Get node text including spaces
     */
    String getText(ParserRuleContext ctx) {
        return ctx.start.getInputStream().getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    /**
     * Trace information
     */
    void trace(ParserRuleContext ctx, String message) {
        exec.trace(ctx, message);
    }
}
