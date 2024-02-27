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

package org.apache.doris.catalog;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.cloud.catalog.CloudEnvFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class EnvFactory {

    public EnvFactory() {}

    private static class SingletonHolder {
        private static final EnvFactory INSTANCE =
                Config.isCloudMode() ? new CloudEnvFactory() : new EnvFactory();
    }

    public static EnvFactory getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public Env createEnv(boolean isCheckpointCatalog) {
        return new Env(isCheckpointCatalog);
    }

    public InternalCatalog createInternalCatalog() {
        return new InternalCatalog();
    }

    public SystemInfoService createSystemInfoService() {
        return new SystemInfoService();
    }

    public Type getPartitionClass() {
        return Partition.class;
    }

    public Partition createPartition() {
        return new Partition();
    }

    public Type getTabletClass() {
        return Tablet.class;
    }

    public Tablet createTablet() {
        return new Tablet();
    }

    public Tablet createTablet(long tabletId) {
        return new Tablet(tabletId);
    }

    public Replica createReplica() {
        return new Replica();
    }

    public ReplicaAllocation createDefReplicaAllocation() {
        return new ReplicaAllocation((short) 3);
    }

    public PropertyAnalyzer createPropertyAnalyzer() {
        return new PropertyAnalyzer();
    }

    public DynamicPartitionProperty createDynamicPartitionProperty(Map<String, String> properties) {
        return new DynamicPartitionProperty(properties);
    }

    public GlobalTransactionMgrIface createGlobalTransactionMgr(Env env) {
        return new GlobalTransactionMgr(env);
    }

    public BrokerLoadJob createBrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt,
            UserIdentity userInfo) throws MetaNotFoundException {
        return new BrokerLoadJob(dbId, label, brokerDesc, originStmt, userInfo);
    }

    public BrokerLoadJob createBrokerLoadJob() {
        return new BrokerLoadJob();
    }

    public Coordinator createCoordinator(ConnectContext context, Analyzer analyzer, Planner planner,
                                         StatsErrorEstimator statsErrorEstimator) {
        return new Coordinator(context, analyzer, planner, statsErrorEstimator);
    }

    public Coordinator createCoordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                         List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                         String timezone, boolean loadZeroTolerance) {
        return new Coordinator(jobId, queryId, descTable, fragments, scanNodes, timezone, loadZeroTolerance);
    }
}
