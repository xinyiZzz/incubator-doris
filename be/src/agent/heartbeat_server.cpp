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

#include "agent/heartbeat_server.h"

#include <thrift/TProcessor.h>

#include <ctime>
#include <filesystem>
#include <fstream>

#include "common/status.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/Status_types.h"
#include "olap/storage_engine.h"
#include "olap/utils.h"
#include "runtime/heartbeat_flags.h"
#include "service/backend_options.h"
#include "util/debug_util.h"
#include "util/thrift_server.h"
#include "util/time.h"

using std::fstream;
using std::nothrow;
using std::string;
using std::vector;
using apache::thrift::transport::TProcessor;

namespace doris {

HeartbeatServer::HeartbeatServer(ExecEnv* exec_env, TMasterInfo* master_info)
        : _exec_env(exec_env), _master_info(master_info), _fe_epoch(0) {
    _olap_engine = StorageEngine::instance();
    _be_epoch = GetCurrentTimeMicros() / 1000;
}

void HeartbeatServer::init_cluster_id() {
    _master_info->cluster_id = _olap_engine->effective_cluster_id();
}

void HeartbeatServer::heartbeat(THeartbeatResult& heartbeat_result,
                                const TMasterInfo& master_info) {
    //print heartbeat in every minute
    LOG_EVERY_N(INFO, 12) << "get heartbeat from FE."
                          << "host:" << master_info.network_address.hostname
                          << ", port:" << master_info.network_address.port
                          << ", cluster id:" << master_info.cluster_id
                          << ", counter:" << google::COUNTER;

    // do heartbeat
    Status st = _heartbeat(master_info);
    st.to_thrift(&heartbeat_result.status);

    if (st.ok()) {
        heartbeat_result.backend_info.__set_be_port(config::be_port);
        heartbeat_result.backend_info.__set_http_port(config::webserver_port);
        heartbeat_result.backend_info.__set_be_rpc_port(-1);
        heartbeat_result.backend_info.__set_brpc_port(config::brpc_port);
        heartbeat_result.backend_info.__set_version(get_short_version());
        heartbeat_result.backend_info.__set_be_start_time(_be_epoch);
    }
}

Status HeartbeatServer::_heartbeat(const TMasterInfo& master_info) {
    std::lock_guard<std::mutex> lk(_hb_mtx);

    if (master_info.__isset.frontends_info) {
        std::stringstream ss;
        ss << "Heartbeat frontends info len: " << master_info.frontends_info.size();
        for (auto info: master_info.frontends_info) {
            ss << "; host:" << info.network_address.hostname
            << ", port:" << info.network_address.port
            << ", fe_start_time:" << info.fe_start_time
            << ", is_alive:" << info.is_alive;
            if (_exec_env->frontends_start_time().find(info.network_address.hostname)
                    != _exec_env->frontends_start_time().end()) {
                FrontendStartInfo* frontend_start_info = _exec_env->frontends_start_time()[info.network_address.hostname];
                frontend_start_info->is_alive = info.is_alive;
                frontend_start_info->start_time = info.fe_start_time;
                frontend_start_info->last_heartbeat->from_unixtime(time(NULL), TimezoneUtils::default_time_zone);
            } else {
                _exec_env->frontends_start_time().insert({info.network_address.hostname
                        , new FrontendStartInfo(info.fe_start_time, info.is_alive
                        , new DateTimeValue(DateTimeValue::local_time().to_int64()))});
            }
        }
        LOG_EVERY_N(INFO, 12) << ss.str();
        _exec_env->last_heartbeat()->from_unixtime(time(NULL), TimezoneUtils::default_time_zone);
    }

    if (master_info.__isset.backend_ip) {
        if (master_info.backend_ip != BackendOptions::get_localhost()) {
            LOG(WARNING) << "backend ip saved in master does not equal to backend local ip"
                         << master_info.backend_ip << " vs. " << BackendOptions::get_localhost();
            std::stringstream ss;
            ss << "actual backend local ip: " << BackendOptions::get_localhost();
            return Status::InternalError(ss.str());
        }
    }

    // Check cluster id
    if (_master_info->cluster_id == -1) {
        LOG(INFO) << "get first heartbeat. update cluster id";
        // write and update cluster id
        auto st = _olap_engine->set_cluster_id(master_info.cluster_id);
        if (!st.ok()) {
            LOG(WARNING) << "fail to set cluster id. status=" << st.get_error_msg();
            return Status::InternalError("fail to set cluster id.");
        } else {
            _master_info->cluster_id = master_info.cluster_id;
            LOG(INFO) << "record cluster id. host: " << master_info.network_address.hostname
                      << ". port: " << master_info.network_address.port
                      << ". cluster id: " << master_info.cluster_id;
        }
    } else {
        if (_master_info->cluster_id != master_info.cluster_id) {
            OLAP_LOG_WARNING("invalid cluster id: %d. ignore.", master_info.cluster_id);
            return Status::InternalError("invalid cluster id. ignore.");
        }
    }

    bool need_report = false;
    if (_master_info->network_address.hostname != master_info.network_address.hostname ||
        _master_info->network_address.port != master_info.network_address.port) {
        if (master_info.epoch > _fe_epoch) {
            _master_info->network_address.hostname = master_info.network_address.hostname;
            _master_info->network_address.port = master_info.network_address.port;
            _fe_epoch = master_info.epoch;
            need_report = true;
            LOG(INFO) << "master change. new master host: "
                      << _master_info->network_address.hostname
                      << ". port: " << _master_info->network_address.port
                      << ". epoch: " << _fe_epoch;
        } else {
            LOG(WARNING) << "epoch is not greater than local. ignore heartbeat. host: "
                         << _master_info->network_address.hostname
                         << " port: " << _master_info->network_address.port
                         << " local epoch: " << _fe_epoch
                         << " received epoch: " << master_info.epoch;
            return Status::InternalError("epoch is not greater than local. ignore heartbeat.");
        }
    } else {
        // when Master FE restarted, host and port remains the same, but epoch will be increased.
        if (master_info.epoch > _fe_epoch) {
            _fe_epoch = master_info.epoch;
            need_report = true;
            LOG(INFO) << "master restarted. epoch: " << _fe_epoch;
        }
    }

    if (master_info.__isset.token) {
        if (!_master_info->__isset.token) {
            _master_info->__set_token(master_info.token);
            LOG(INFO) << "get token. token: " << _master_info->token;
        } else if (_master_info->token != master_info.token) {
            LOG(WARNING) << "invalid token. local_token:" << _master_info->token
                         << ". token:" << master_info.token;
            return Status::InternalError("invalid token.");
        }
    }

    if (master_info.__isset.http_port) {
        _master_info->__set_http_port(master_info.http_port);
    }

    if (master_info.__isset.heartbeat_flags) {
        HeartbeatFlags* heartbeat_flags = _exec_env->heartbeat_flags();
        heartbeat_flags->update(master_info.heartbeat_flags);
    }

    if (master_info.__isset.backend_id) {
        _master_info->__set_backend_id(master_info.backend_id);
    }

    if (need_report) {
        LOG(INFO) << "Master FE is changed or restarted. report tablet and disk info immediately";
        _olap_engine->notify_listeners();
    }

    return Status::OK();
}

const bool HeartbeatServer::is_fe_restart(ExecEnv* exec_env,
                                          const std::string& hostname,
                                          const DateTimeValue& fe_msg_time) {
    // In previous version, if (query_type == TQueryType::LOAD && hostname.empty()) is true.
    if (hostname.empty()) {
        return false;
    }
    // Heartbeat lags behind the frontend message time
    // For example: after the frontend restarts, when the heartbeat is not sent to the backend in time, 
    // the backend may cancel the request sent by the frontend
    if (exec_env->last_heartbeat()->second_diff(fe_msg_time) < 0) {
        return false;
    }
    int64_t start_timestamp;
    fe_msg_time.unix_timestamp(&start_timestamp, TimezoneUtils::default_time_zone);
    if (exec_env->frontends_start_time().find(hostname) == exec_env->frontends_start_time().end()) {
        LOG(WARNING) << "FE hostname: " << hostname << " not exist";
        return true;
    } else {
        FrontendStartInfo* fe_start_info = exec_env->frontends_start_time().find(hostname)->second;
        // The info of this frontend is not received in the recent heartbeat. At this time, the info is invalid
        if (fe_start_info->last_heartbeat->second_diff(fe_msg_time) < 0) {
            return false;
        }
        if (fe_start_info->is_alive == false || fe_start_info->start_time == -1) {
            LOG(WARNING) << "FE hostname: " << hostname << " not alive";
            return true;
        } else if (start_timestamp * 1000 < fe_start_info->start_time) {
            LOG(WARNING) << "The query submitted time: " << fe_msg_time << " before this frontend: "
                         << hostname << " restart, should be cancelled";
            return true;
        }
    }
    return false;
}

AgentStatus create_heartbeat_server(ExecEnv* exec_env, uint32_t server_port,
                                    ThriftServer** thrift_server, uint32_t worker_thread_num,
                                    TMasterInfo* local_master_info) {
    HeartbeatServer* heartbeat_server = new (nothrow) HeartbeatServer(exec_env, local_master_info);
    if (heartbeat_server == NULL) {
        return DORIS_ERROR;
    }

    heartbeat_server->init_cluster_id();

    std::shared_ptr<HeartbeatServer> handler(heartbeat_server);
    std::shared_ptr<TProcessor> server_processor(new HeartbeatServiceProcessor(handler));
    string server_name("heartbeat");
    *thrift_server =
            new ThriftServer(server_name, server_processor, server_port, worker_thread_num);
    return DORIS_SUCCESS;
}
} // namespace doris
