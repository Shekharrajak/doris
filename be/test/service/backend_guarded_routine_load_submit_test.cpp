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

#include <gtest/gtest.h>
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <memory>
#include <string>
#include <vector>

#include "agent/utils.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/env_config.h"
#include "load/routine_load/routine_load_task_executor.h"
#include "load/stream_load/stream_load_context.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "service/backend_service.h"

namespace doris {

namespace {
class RecordingBackendService : public BaseBackendService {
public:
    explicit RecordingBackendService(ExecEnv* env) : BaseBackendService(env) {}

    Status start_thrift_dependencies() override { return Status::OK(); }

    void submit_routine_load_task(TStatus& result,
                                  const std::vector<TRoutineLoadTask>& tasks) override {
        ++ordinary_calls;
        BaseBackendService::submit_routine_load_task(result, tasks);
    }

    void submit_guarded_routine_load_task(TStatus& result,
                                          const std::vector<TRoutineLoadTask>& tasks) override {
        ++guarded_calls;
        BaseBackendService::submit_guarded_routine_load_task(result, tasks);
    }

    int ordinary_calls = 0;
    int guarded_calls = 0;
};
} // namespace

class BackendGuardedRoutineLoadSubmitTest : public testing::Test {
public:
    void SetUp() override {
        _filtering = config::enable_kafka_share_completion_filtering;
        _deploy_mode = config::deploy_mode;
        _cloud_unique_id = config::cloud_unique_id;
        config::enable_kafka_share_completion_filtering = true;
        config::deploy_mode = "local";
        config::cloud_unique_id.clear();
        _env = ExecEnv::GetInstance();
        _previous_cluster_info = _env->cluster_info();
        _previous_executor = _env->routine_load_task_executor();
        _env->set_cluster_info(&_cluster_info);
        _executor = std::make_unique<RoutineLoadTaskExecutor>(_env);
        _env->set_routine_load_task_executor(_executor.get());
        _service = std::make_shared<RecordingBackendService>(_env);
    }

    void TearDown() override {
        _service.reset();
        _executor->stop();
        _executor.reset();
        _env->set_routine_load_task_executor(_previous_executor);
        _env->set_cluster_info(_previous_cluster_info);
        MasterServerClient::create(_previous_cluster_info);
        config::enable_kafka_share_completion_filtering = _filtering;
        config::deploy_mode = _deploy_mode;
        config::cloud_unique_id = _cloud_unique_id;
    }

    TRoutineLoadTask task() {
        TRoutineLoadTask result;
        result.__set_type(TLoadSourceType::KAFKA);
        result.__set_job_id(7);
        result.__set_txn_id(11);
        result.__set_id(UniqueId::gen_uid().to_thrift());
        result.__set_kafka_source_guard_version(KafkaLoadInfo::SOURCE_GUARD_VERSION);
        TKafkaLoadInfo info;
        info.__set_brokers("unused:9092");
        info.__set_topic("unused");
        info.__set_use_share_consumer(true);
        result.__set_kafka_load_info(info);
        return result;
    }

    ExecEnv* _env = nullptr;
    ClusterInfo _cluster_info;
    ClusterInfo* _previous_cluster_info = nullptr;
    RoutineLoadTaskExecutor* _previous_executor = nullptr;
    std::unique_ptr<RoutineLoadTaskExecutor> _executor;
    std::shared_ptr<RecordingBackendService> _service;
    bool _filtering = false;
    std::string _deploy_mode;
    std::string _cloud_unique_id;
};

TEST_F(BackendGuardedRoutineLoadSubmitTest, MissingOrUnsupportedVersionNeverQueues) {
    auto request = task();
    for (int32_t version : {0, KafkaLoadInfo::SOURCE_GUARD_VERSION + 1}) {
        request.__set_kafka_source_guard_version(version);
        request.__isset.kafka_source_guard_version = version != 0;
        TStatus result;
        _service->submit_guarded_routine_load_task(result, {request});
        EXPECT_EQ(result.status_code, TStatusCode::INVALID_ARGUMENT);
        EXPECT_TRUE(_executor->_task_map.empty());
    }
    EXPECT_EQ(_service->ordinary_calls, 0);
}

#ifdef HAVE_RDKAFKA_MESSAGE_TOPIC_ID
TEST_F(BackendGuardedRoutineLoadSubmitTest, WholeListPreflightRejectsLaterTaskBeforeAnyQueue) {
    auto first = task();
    ASSERT_TRUE(RoutineLoadTaskExecutor::validate_task(first, KafkaLoadInfo::SOURCE_GUARD_VERSION)
                        .ok());
    auto second = task();
    second.__set_kafka_source_guard_version(KafkaLoadInfo::SOURCE_GUARD_VERSION + 1);
    TStatus result;
    _service->submit_guarded_routine_load_task(result, {first, second});
    EXPECT_EQ(result.status_code, TStatusCode::INVALID_ARGUMENT);
    ASSERT_EQ(result.error_msgs.size(), 1);
    EXPECT_NE(result.error_msgs[0].find("Unsupported Kafka source guard version"),
              std::string::npos);
    EXPECT_TRUE(_executor->_task_map.empty());
    EXPECT_EQ(_service->ordinary_calls, 0);
}
#else
TEST_F(BackendGuardedRoutineLoadSubmitTest, UnavailableIdentityApiNeverQueues) {
    TStatus result;
    _service->submit_guarded_routine_load_task(result, {task()});
    EXPECT_NE(result.status_code, TStatusCode::OK);
    ASSERT_EQ(result.error_msgs.size(), 1);
    EXPECT_NE(result.error_msgs[0].find("delivered topic identity API"), std::string::npos);
    EXPECT_TRUE(_executor->_task_map.empty());
    EXPECT_EQ(_service->ordinary_calls, 0);
}
#endif

TEST_F(BackendGuardedRoutineLoadSubmitTest, LegacyProcessorRejectsUnknownMethodWithoutFallback) {
    auto request_buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    auto response_buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    auto request_protocol =
            std::make_shared<apache::thrift::protocol::TBinaryProtocol>(request_buffer);
    auto response_protocol =
            std::make_shared<apache::thrift::protocol::TBinaryProtocol>(response_buffer);
    BackendServiceClient client(response_protocol, request_protocol);
    BackendServiceProcessor processor(_service);
    ASSERT_EQ(processor.processMap_.erase("submit_guarded_routine_load_task"), 1);

    client.send_submit_guarded_routine_load_task({task()});
    ASSERT_TRUE(processor.process(request_protocol, response_protocol, nullptr));
    try {
        TStatus result;
        client.recv_submit_guarded_routine_load_task(result);
        FAIL() << "Legacy processor accepted a guarded submit";
    } catch (const apache::thrift::TApplicationException& error) {
        EXPECT_EQ(error.getType(), apache::thrift::TApplicationException::UNKNOWN_METHOD);
    }
    EXPECT_EQ(_service->guarded_calls, 0);
    EXPECT_EQ(_service->ordinary_calls, 0);
    EXPECT_TRUE(_executor->_task_map.empty());
}

} // namespace doris
