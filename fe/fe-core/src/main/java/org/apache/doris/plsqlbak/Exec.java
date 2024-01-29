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

package org.apache.doris.plsqlbak;

import org.apache.doris.hplsql.Arguments;
import org.apache.doris.hplsql.Conf;
import org.apache.doris.hplsql.Conn;
import org.apache.doris.hplsql.Console;
import org.apache.doris.hplsql.Converter;
import org.apache.doris.hplsql.Meta;
import org.apache.doris.hplsql.Package;
import org.apache.doris.hplsql.Query;
import org.apache.doris.hplsql.executor.QueryExecutor;
import org.apache.doris.hplsql.executor.QueryResult;
import org.apache.doris.hplsql.executor.ResultListener;
import org.apache.doris.hplsql.functions.BuiltinFunctions;
import org.apache.doris.hplsql.objects.TableClass;
import org.apache.doris.hplsql.packages.DorisPackageRegistry;
import org.apache.doris.hplsql.packages.InMemoryPackageRegistry;
import org.apache.doris.hplsql.packages.PackageRegistry;
import org.apache.doris.hplsql.store.MetaClient;
import org.apache.doris.nereids.DorisParser.CallProcedureContext;
import org.apache.doris.nereids.parser.plsql.PLSqlLogicalPlanBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.plsqlbak.Var.Type;
import org.apache.doris.plsqlbak.functions.DorisFunctionRegistry;
import org.apache.doris.qe.ConnectContext;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

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
    public Stack<String> labels = new Stack<>();
    Stack<String> callStack = new Stack<>();

    Stack<Signal> signals = new Stack<>();
    Signal currentSignal;
    Scope currentHandlerScope;
    boolean resignal = false;

    HashMap<String, String> managedTables = new HashMap<>();
    HashMap<String, String> objectMap = new HashMap<>();
    HashMap<String, String> objectConnMap = new HashMap<>();
    HashMap<String, ArrayList<String>> returnCursors = new HashMap<>();
    HashMap<String, Package> packages = new HashMap<>();

    Package currentPackageDecl = null;

    public ArrayList<String> stmtConnList = new ArrayList<>();

    Arguments arguments = new Arguments();
    public Conf conf;
    Expression expr;
    Converter converter;
    Meta meta;
    public Select select;
    public Stmt stmt;
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

    // ProcedureStatementContext lastStmt = null;
    PLSqlLogicalPlanBuilder logicalPlanBuilder;

    public Exec() {
        exec = this;
        // queryExecutor = new JdbcQueryExecutor(this); // hpl-sql, hplsql.sh走的这 // 对的
    }

    public Exec(Conf conf, Console console, QueryExecutor queryExecutor, ResultListener resultListener) {
        this.conf = conf;
        this.exec = this;
        this.console = console;
        this.queryExecutor = queryExecutor; // 什么时候用doris executor，什么时候用jdbc executor
        this.resultListener = resultListener;
        this.client = new MetaClient();
    }

    Exec(Exec exec) {
        this.exec = exec;
        this.console = exec.console;
        this.queryExecutor = exec.queryExecutor;
        this.client = exec.client;
    }

    @Override
    public void close() {
        // leaveScope();
        // cleanup();
        // printExceptions();
    }

    public Integer visitChildren(ParserRuleContext ctx) {
        return logicalPlanBuilder.visitChildrenReal(ctx);
    }

    /**
     * Initialize PL/HQL
     */
    public void init() {
        enterGlobalScope();
        // specify the default log4j2 properties file.
        // System.setProperty("log4j.configurationFile", "hive-log4j2.properties");
        // if (conf == null) {
        //     conf = new Conf();
        // }
        // conf.init();
        // conn = new Conn(this);
        // meta = new Meta(this, queryExecutor);
        // initOptions();
        logicalPlanBuilder = new PLSqlLogicalPlanBuilder();

        // expr = new Expression(this);
        select = new Select(this, queryExecutor);
        select.setResultListener(resultListener);
        stmt = new Stmt(this, queryExecutor);
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
            List<Expression> arguments, ConnectContext connectContext) {
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
     * Executing a statement
     */
    public Integer visitStmt(ProcedureStatementContext ctx) {
        // if (ctx.semicolon_stmt() != null) {
        //     return 0;
        // }
        // if (initRoutines && ctx.create_procedure_stmt() == null && ctx.create_function_stmt() == null) {
        //     return 0;
        // }
        // if (exec.resignal) {
        //     if (exec.currentScope != exec.currentHandlerScope.parent) {
        //         return 0;
        //     }
        //     exec.resignal = false;
        // }
        // if (!exec.signals.empty() && exec.conf.onError != org.apache.doris.hplsql.Exec.OnError.SETERROR) {
        //     if (!runContinueHandler()) {
        //         return 0;
        //     }
        // }
        // Var prev = stackPop();
        // if (prev != null && prev.value != null) {
        //     console.printLine(prev.toString());
        // }
        Integer rc = visitChildren(ctx);
        if (ctx != lastStmt) { // 这里的 lastStmt，是指用分号分割的最后一个么 // 一条语句中分号分割的多个sql 在mysql中就是为了
            // 支持存储过程，不过我们现在支持的是拆成了多个SQL，// 如果三条语句 xx;xx;xx; 前两条返回OK，最后一条返回EOF
            // printExceptions();
            resultListener.onFinalize(); // 是用 mysql client 执行 hqlsql，走这个 // 有些情况是不需要经过反序列化再返回mysql
            // client，比如 select xxx from tbl; 此时hplsql没有计算，比如 select count(*) into result 有 result
            console.flushConsole(); // 用 hplsql.sh执行 hqlsql
        }
        return rc;
    }

    /**
     * CALL statement
     */
    public Integer visitCall_stmt(ConnectContext connectContext, CallProcedureContext ctx, String functionName,
            List<Expression> arguments) {
        exec.inCallStmt = true;
        try {
            // if (ctx.expr_func() != null) {
            functionCall(ctx, functionName, arguments, connectContext);
            // } else if (ctx.expr_dot() != null) {
            //     visitExpr_dot(ctx.expr_dot());
            // } else if (ctx.ident_pl() != null) {
            //     functionCall(ctx, ctx.ident_pl(), null);
            // }
        } finally {
            exec.inCallStmt = false;
        }
        return 0;
    }

    /**
     * Enter a new scope
     */
    public void enterScope(Scope scope) {
        exec.scopes.push(scope);
    }

    public void enterScope(Scope.Type type) {
        enterScope(type, null);
    }

    public void enterScope(Scope.Type type, Package pack) {
        exec.currentScope = new Scope(exec.currentScope, type, pack);
        enterScope(exec.currentScope);
    }

    public void enterGlobalScope() {
        globalScope = new Scope(Scope.Type.GLOBAL);
        currentScope = globalScope;
        enterScope(globalScope);
    }

    /**
     * Leave the current scope
     */
    public void leaveScope() {
        if (!exec.signals.empty()) {
            Scope scope = exec.scopes.peek();
            Signal signal = exec.signals.peek();
            // if (exec.conf.onError != org.apache.doris.hplsql.Exec.OnError.SETERROR) {
            //     runExitHandler();
            // }
            if (signal.type == Signal.Type.LEAVE_ROUTINE && scope.type == Scope.Type.ROUTINE) {
                exec.signals.pop();
            }
        }
        exec.currentScope = exec.scopes.pop().getParent();
    }

    /**
     * Send a signal
     */
    public void signal(Signal signal) {
        exec.signals.push(signal);
    }

    public void signal(Signal.Type type, String value, Exception exception) {
        signal(new Signal(type, value, exception));
    }

    public void signal(Signal.Type type, String value) {
        // setSqlCode(SqlCodes.ERROR);
        signal(type, value, null);
    }

    public void signal(Signal.Type type) {
        // setSqlCode(SqlCodes.ERROR);
        signal(type, null, null);
    }

    public void signal(Query query) {
        // setSqlCode(query.getException());
        signal(Signal.Type.SQLEXCEPTION, query.errorText(), query.getException());
    }

    public void signal(QueryResult query) {
        // setSqlCode(query.exception());
        signal(Signal.Type.SQLEXCEPTION, query.errorText(), query.exception());
    }

    public void signal(Exception exception) {
        // setSqlCode(exception);
        signal(Signal.Type.SQLEXCEPTION, exception.getMessage(), exception);
    }

    /**
     * Pop the last signal
     */
    public Signal signalPop() {
        if (!exec.signals.empty()) {
            return exec.signals.pop();
        }
        return null;
    }

    /**
     * Peek the last signal
     */
    public Signal signalPeek() {
        if (!exec.signals.empty()) {
            return exec.signals.peek();
        }
        return null;
    }

    public String labelPop() {
        if (!exec.labels.empty()) {
            return exec.labels.pop();
        }
        return "";
    }

    /**
     * Add a local variable to the current scope
     */
    public void addVariable(Var var) {
        // if (currentPackageDecl != null) {
        //     currentPackageDecl.addVariable(var);
        // } else if (exec.currentScope != null) {
        if (exec.currentScope != null) {
            exec.currentScope.addVariable(var);
        }
    }

    /**
     * Find an existing variable by name
     */
    public Var findVariable(String name) {
        Var var;
        String name1 = name.toUpperCase();
        String name1a = null;
        // String name2 = null;
        Scope cur = exec.currentScope;
        // Package pack;
        // Package packCallContext = exec.getPackageCallContext();
        // ArrayList<String> qualified = exec.meta.splitIdentifier(name);
        // if (qualified != null) {
        //     name1 = qualified.get(0).toUpperCase();
        //     name2 = qualified.get(1).toUpperCase();
        //     pack = findPackage(name1);
        //     if (pack != null) {
        //         var = pack.findVariable(name2);
        //         if (var != null) {
        //             return var;
        //         }
        //     }
        // }
        if (name1.startsWith(":")) {
            name1a = name1.substring(1);
        }
        while (cur != null) {
            var = findVariable(cur.vars, name1);
            if (var == null && name1a != null) {
                var = findVariable(cur.vars, name1a);
            }
            // if (var == null && packCallContext != null) {
            //     var = packCallContext.findVariable(name1);
            // }
            if (var != null) {
                return var;
            }
            if (cur.type == Scope.Type.ROUTINE) {
                cur = exec.globalScope;
            } else {
                cur = cur.parent;
            }
        }
        return null;
    }

    // public Var findVariable(Var name) {
    //     return findVariable(name.getName());
    // }
    //
    Var findVariable(Map<String, Var> vars, String name) {
        return vars.get(name.toUpperCase());
    }

    /**
     * Find a cursor variable by name
     */
    public Var findCursor(String name) {
        Var cursor = exec.findVariable(name);
        if (cursor != null && cursor.type == Type.CURSOR) {
            return cursor;
        }
        return null;
    }

    /**
     * Push a value to the stack
     */
    public void stackPush(Var var) {
        exec.stack.push(var);
    }

    /**
     * Push a string value to the stack
     */
    public void stackPush(String val) {
        exec.stack.push(new Var(val));
    }

    public void stackPush(StringBuilder val) {
        stackPush(val.toString());
    }

    /**
     * Select a value from the stack, but not remove
     */
    public Var stackPeek() {
        return exec.stack.peek();
    }

    /**
     * Pop a value from the stack
     */
    public Var stackPop() {
        if (!exec.stack.isEmpty()) {
            return exec.stack.pop();
        }
        return Var.Empty;
    }

    /**
     * Append the text preserving the formatting (space symbols) between tokens
     */
    void append(StringBuilder str, String appendStr, Token start, Token stop) {
        String spaces = start.getInputStream()
                .getText(new org.antlr.v4.runtime.misc.Interval(start.getStartIndex(), stop.getStopIndex()));
        spaces = spaces.substring(start.getText().length(), spaces.length() - stop.getText().length());
        str.append(spaces);
        str.append(appendStr);
    }

    void append(StringBuilder str, TerminalNode start, TerminalNode stop) {
        String text = start.getSymbol().getInputStream().getText(
                new org.antlr.v4.runtime.misc.Interval(start.getSymbol().getStartIndex(),
                        stop.getSymbol().getStopIndex()));
        str.append(text);
    }

    public boolean getOffline() {
        return exec.offline;
    }
}
