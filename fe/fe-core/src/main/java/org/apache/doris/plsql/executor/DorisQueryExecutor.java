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

package org.apache.doris.plsql.executor;

import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.plsql.exception.QueryException;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.MysqlConnectProcessor;
import org.apache.doris.qe.StmtExecutor;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DorisQueryExecutor implements QueryExecutor { // 这个不应该放到hpl目录下，应该放到一个单独的doris目录，保持原有
    // 目录结构 文件和原来hpl包相同，主要改的是executor 和 store 这两个目录对吧
    public DorisQueryExecutor() { // 改名叫 HplSqlQueryExecutor
    }

    @Override
    public QueryResult executeQuery(String sql, ParserRuleContext ctx) {
        // A cursor may correspond to a query, and if the user opens multiple cursors, need to save multiple
        // query states, so here each query constructs a ConnectProcessor and the ConnectContext shares some data.
        // 为啥每次都copy一个ConnectContext? hplsql支持同时执行多个语句，通过cursor同时保存多个sql的结果
        ConnectContext context = ConnectContext.get().createContext();
        try (AutoCloseConnectContext autoCloseCtx = new AutoCloseConnectContext(context)) {
            autoCloseCtx.call();
            context.setRunProcedure(true);
            ConnectProcessor processor = new MysqlConnectProcessor(context);
            // 可能可以直接 executor = new StmtExecutor(ctx, parsedStmt);，execute // 可能不用每条语句都 finalizeCommand
            processor.executeQuery(MysqlCommand.COM_QUERY, sql);
            StmtExecutor executor = context.getExecutor();
            return new QueryResult(new DorisRowResult(executor.getCoord(), executor.getColumns(),
                    executor.getReturnTypes()), () -> metadata(executor), processor, null);
        } catch (Exception e) {
            return new QueryResult(null, () -> new Metadata(Collections.emptyList()), null, e);
        }
    }

    private Metadata metadata(StmtExecutor stmtExecutor) {
        try {
            List<String> columns = stmtExecutor.getColumns();
            List<Type> types = stmtExecutor.getReturnTypes();
            List<ColumnMeta> colMeta = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                PrimitiveType primitiveType = types.get(i).getPrimitiveType();
                MysqlColType mysqlColType = primitiveType.toMysqlType();
                colMeta.add(new ColumnMeta(columns.get(i), mysqlColType.getTypeName(), primitiveType.toJavaSqlType(),
                        types.get(i)));
            }
            return new Metadata(colMeta);
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }
}
