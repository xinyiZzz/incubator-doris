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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Exec.java
// and modified by Doris

package org.apache.doris.procedure;

import org.apache.doris.hplsql.Arguments;
import org.apache.doris.hplsql.Conf;
import org.apache.doris.hplsql.Conn;
import org.apache.doris.hplsql.Console;
import org.apache.doris.hplsql.Converter;
import org.apache.doris.hplsql.Expression;
import org.apache.doris.hplsql.HplsqlParser.StmtContext;
import org.apache.doris.hplsql.Meta;
import org.apache.doris.hplsql.Package;
import org.apache.doris.hplsql.Scope;
import org.apache.doris.hplsql.Signal;
import org.apache.doris.hplsql.Stmt;
import org.apache.doris.hplsql.Var;
import org.apache.doris.hplsql.executor.QueryExecutor;
import org.apache.doris.hplsql.executor.ResultListener;
import org.apache.doris.hplsql.functions.BuiltinFunctions;
import org.apache.doris.hplsql.objects.TableClass;
import org.apache.doris.hplsql.packages.DorisPackageRegistry;
import org.apache.doris.hplsql.packages.InMemoryPackageRegistry;
import org.apache.doris.hplsql.packages.PackageRegistry;
import org.apache.doris.hplsql.store.MetaClient;
import org.apache.doris.nereids.DorisParser.CallProcedureContext;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.procedure.functions.DorisFunctionRegistry;
import org.apache.doris.procedure.functions.FunctionRegistry;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * HPL/SQL script executor
 */
public class Exec implements Closeable {

    public static final String VERSION = "HPL/SQL 0.3.31";
    public static final String ERRORCODE = "ERRORCODE";
    public static final String SQLCODE = "SQLCODE";
    public static final String SQLSTATE = "SQLSTATE";
    public static final String HOSTCODE = "HOSTCODE";

    Exec exec;
    FunctionRegistry functions;
    private BuiltinFunctions builtinFunctions;
    private MetaClient client;
    QueryExecutor queryExecutor;
    private PackageRegistry packageRegistry = new InMemoryPackageRegistry();
    private boolean packageLoading = false;
    private Map<String, TableClass> types = new HashMap<>();

    public enum OnError {
        EXCEPTION, SETERROR, STOP
    }

    // Scopes of execution (code blocks) with own local variables, parameters and exception handlers
    Stack<Scope> scopes = new Stack<>();
    Scope globalScope;
    Scope currentScope;

    Stack<Var> stack = new Stack<>();
    Stack<String> labels = new Stack<>();
    Stack<String> callStack = new Stack<>();

    Stack<Signal> signals = new Stack<>();
    Signal currentSignal;
    Scope currentHandlerScope;
    boolean resignal = false;

    HashMap<String, String> managedTables = new HashMap<>();
    HashMap<String, String> objectMap = new HashMap<>();
    HashMap<String, String> objectConnMap = new HashMap<>();
    HashMap<String, ArrayList<Var>> returnCursors = new HashMap<>();
    HashMap<String, Package> packages = new HashMap<>();

    Package currentPackageDecl = null;

    public ArrayList<String> stmtConnList = new ArrayList<>();

    Arguments arguments = new Arguments();
    public Conf conf;
    Expression expr;
    Converter converter;
    Meta meta;
    public Select select;
    Stmt stmt;
    Conn conn;
    Console console = Console.STANDARD;
    ResultListener resultListener = ResultListener.NONE;

    int rowCount = 0;

    StringBuilder localUdf = new StringBuilder();
    boolean initRoutines = false;
    public boolean buildSql = false;
    public boolean inCallStmt = false;
    boolean udfRegistered = false;
    boolean udfRun = false;

    boolean dotHplsqlrcExists = false;
    boolean hplsqlrcExists = false;

    boolean trace = false;
    boolean info = true;
    boolean offline = false;
    public LogicalPlanBuilder logicalPlanBuilder;

    StmtContext lastStmt = null;

    public Exec() {
        exec = this;
        // queryExecutor = new JdbcQueryExecutor(this); // hpl-sql, hplsql.sh走的这 // 对的
        this.logicalPlanBuilder = new LogicalPlanBuilder();
    }

    public Exec(Conf conf, Console console, QueryExecutor queryExecutor, ResultListener resultListener,
            LogicalPlanBuilder logicalPlanBuilder) {
        this.conf = conf;
        this.exec = this;
        this.console = console;
        this.queryExecutor = queryExecutor; // 什么时候用doris executor，什么时候用jdbc executor
        this.resultListener = resultListener;
        this.client = new MetaClient();
        this.logicalPlanBuilder = logicalPlanBuilder;
    }

    Exec(Exec exec) {
        this.exec = exec;
        this.console = exec.console;
        this.queryExecutor = exec.queryExecutor;
        this.client = exec.client;
        this.logicalPlanBuilder = new LogicalPlanBuilder();
    }

    @Override
    public void close() {
        // leaveScope();
        // cleanup();
        // printExceptions();
    }

    /**
     * Initialize PL/HQL
     */
    public void init() {
        // enterGlobalScope();
        // specify the default log4j2 properties file.
        // System.setProperty("log4j.configurationFile", "hive-log4j2.properties");
        // if (conf == null) {
        //     conf = new Conf();
        // }
        // conf.init();
        // conn = new Conn(this);
        // meta = new Meta(this, queryExecutor);
        // initOptions();

        // expr = new Expression(this);
        select = new Select(this, queryExecutor);
        select.setResultListener(resultListener);
        // stmt = new Stmt(this, queryExecutor);
        // converter = new Converter(this);
        //
        // builtinFunctions = new BuiltinFunctions(this, queryExecutor);
        // new FunctionDatetime(this, queryExecutor).register(builtinFunctions);
        // new FunctionMisc(this, queryExecutor).register(builtinFunctions);
        // new FunctionString(this, queryExecutor).register(builtinFunctions);
        if (client != null) {
            functions = new DorisFunctionRegistry(this, client, builtinFunctions);
            packageRegistry = new DorisPackageRegistry(client);
        } else {
            // functions = new InMemoryFunctionRegistry(this, builtinFunctions);
        }
        // addVariable(new Var(ERRORCODE, Type.BIGINT, 0L));
        // addVariable(new Var(SQLCODE, Type.BIGINT, 0L));
        // addVariable(new Var(SQLSTATE, Type.STRING, "00000"));
        // addVariable(new Var(HOSTCODE, Type.BIGINT, 0L));
        // for (Entry<String, String> v : arguments.getVars().entrySet()) {
        //     addVariable(new Var(v.getKey(), Type.STRING, v.getValue()));
        // }
        // includeRcFile();
        // registerBuiltins();
    }

    private int functionCall(CallProcedureContext ctx, String name,
            List<org.apache.doris.nereids.trees.expressions.Expression> arguments, ConnectContext connectContext) {
        // if (exec.buildSql) {
        //     exec.execSql(name, params);
        // } else {
        name = name.toUpperCase();
        // Package packCallContext = exec.getPackageCallContext();
        // ArrayList<String> qualified = exec.meta.splitIdentifier(name);
        // boolean executed = false;
        // if (qualified != null) {
        //     Package pack = findPackage(qualified.get(0));
        //     if (pack != null) {
        //         executed = pack.execFunc(qualified.get(1), params);
        //     }
        // }
        // if (!executed && packCallContext != null) {
        //     executed = packCallContext.execFunc(name, params);
        // }
        // if (!executed) {
        if (!exec.functions.exec(name, arguments, connectContext)) {
            // Var var = findVariable(name);
            // if (var != null && var.type == Type.HPL_OBJECT) {
            //     stackPush(dispatch(ctx, (HplObject) var.value, MethodDictionary.__GETITEM__, params));
            // } else {
            //     throw new UndefinedIdentException(ctx, name);
            // }
        }
        // }
        // }
        return 0;
    }

    /**
     * CALL statement
     */
    public Integer visitCall_stmt(CallProcedureContext ctx, ConnectContext connectContext) {
        String functionName = ctx.functionName.getText();
        List<org.apache.doris.nereids.trees.expressions.Expression> arguments = ctx.expression().stream()
                .<org.apache.doris.nereids.trees.expressions.Expression>map(logicalPlanBuilder::typedVisit)
                .collect(ImmutableList.toImmutableList());

        exec.inCallStmt = true;
        try {
            // if (ctx.expr_func() != null) {
            functionCall(ctx, functionName, arguments, connectContext);
            // } else if (ctx.expr_dot() != null) {
            //     visitExpr_dot(ctx.expr_dot());
            // } else if (ctx.ident() != null) {
            //     functionCall(ctx, ctx.ident(), null);
            // }
        } finally {
            exec.inCallStmt = false;
        }
        return 0;
    }
}
