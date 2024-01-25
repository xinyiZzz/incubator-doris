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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.hplsql.store.MetaClient;
import org.apache.doris.nereids.DorisParser.CreateProcedureContext;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * create table procedure
 */
@Developing
public class CreateProcedureCommand extends Command implements ForwardWithSync {
    public static final Logger LOG = LogManager.getLogger(CreateProcedureCommand.class);

    private MetaClient client;
    private final String name;
    private final String source;
    private final boolean isForce;
    private final CreateProcedureContext ctx;
    private final List<Map<String, DataType>> arguments;

    /**
     * constructor
     */
    public CreateProcedureCommand(String name, String source, boolean isForce, CreateProcedureContext ctx,
            List<Map<String, DataType>> arguments) {
        super(PlanType.CREATE_PROCEDURE_COMMAND);
        this.client = new MetaClient();
        this.name = name;
        this.source = source;
        this.isForce = isForce;
        this.ctx = ctx;
        this.arguments = arguments;
    }

    public CreateProcedureContext getCreateProcedureContext() {
        return ctx;
    }

    public List<Map<String, DataType>> getArguments() {
        return arguments;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        client.addStoredProcedure(name, ctx.getCurrentCatalog().getName(), ctx.getDatabase(), ctx.getQualifiedUser(),
                source, isForce);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateProcedureCommand(this, context);
    }
}
