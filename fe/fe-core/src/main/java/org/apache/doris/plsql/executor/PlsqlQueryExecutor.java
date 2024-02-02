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

import org.apache.doris.common.ErrorCode;
import org.apache.doris.plsql.Arguments;
import org.apache.doris.plsql.Conf;
import org.apache.doris.plsql.Exec;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PlsqlQueryExecutor { // 从hpl目录移除 放到doris目录，应该继承自connectProcessor // 移到MysqlConnectProcesser
    private static final Logger LOG = LogManager.getLogger(PlsqlQueryExecutor.class);

    private PlsqlResult result;

    private Exec exec;

    public PlsqlQueryExecutor() {
        result = new PlsqlResult();
        exec = new Exec(new Conf(), result, new DorisQueryExecutor(), result);
        exec.init();
    }

    public void execute(ConnectContext ctx, String statement) {
        ctx.setRunProcedure(true);
        ctx.setProcedureExec(exec);
        result.reset();
        try {
            Arguments args = new Arguments();
            args.parse(new String[] {"-e", statement});
            exec.parseAndEval(args);
            // 这里为什么打印execption?中间有异常但是没有被捕获，比如存储过程中查了一个表不存在，返回空结果，不是异常
            exec.printExceptions();
            String error = result.getError();
            String msg = result.getMsg();
            if (!error.isEmpty()) {
                ctx.getState().setError("hplsql exec error, " + error);
            } else if (!msg.isEmpty()) {
                ctx.getState().setOk(0, 0, msg);
            }
            ctx.getMysqlChannel().reset();
            ctx.getState().setOk();
            ctx.setRunProcedure(false);
            ctx.setProcedureExec(null);
        } catch (Exception e) {
            exec.printExceptions();
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, result.getError() + " " + e.getMessage());
            LOG.warn(e);
            // } finally { // 在后面 printExceptions 可能有问题，可能拿不到错误了，踩过的坑
            //     exec.printExceptions();
        }
    }
}
