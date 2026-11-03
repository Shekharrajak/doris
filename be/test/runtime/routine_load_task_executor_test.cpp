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

#include "load/routine_load/routine_load_task_executor.h"

#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <librdkafka/rdkafkacpp.h>
#include <unistd.h>

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "common/env_config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/stream_load_pipe.h"
#include "load/routine_load/kafka_share_consumer.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_executor.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"

namespace doris {

using namespace RdKafka;

extern TLoadTxnBeginResult k_stream_load_begin_result;
extern TLoadTxnCommitResult k_stream_load_commit_result;
extern TLoadTxnRollbackResult k_stream_load_rollback_result;
extern TStreamLoadPutResult k_stream_load_put_result;
extern TGetKafkaSourceCompletionsResult k_stream_load_source_completions_result;

class TaskShareConsumer : public KafkaShareConsumer {
public:
    enum class BatchKind { PREFIX, ALL_DONE, MIXED_DONE_NEW };

    BatchKind batch_kind = BatchKind::PREFIX;
    bool fail_consume = false;
    bool fail_pipeline = false;
    bool fail_ack = false;
    int cancel_calls = 0;
    int acknowledgement_commits = 0;
    std::vector<std::tuple<int32_t, int64_t, bool>> acknowledgements;
    std::vector<std::string> events;

    bool match(std::shared_ptr<StreamLoadContext> ctx) override {
        return ctx->kafka_info->use_share_consumer;
    }

    Status consume(std::shared_ptr<StreamLoadContext> ctx) override {
        _ctx = ctx;
        _batch.reserve(2);
        _batch.add_record(0, 100);
        _batch.add_record(1, 50);
        if (batch_kind == BatchKind::ALL_DONE) {
            _batch.mark_done(0);
            _batch.mark_done(1);
        } else if (batch_kind == BatchKind::MIXED_DONE_NEW) {
            _batch.mark_done(0);
            _batch.mark_appended(1);
        } else {
            _batch.mark_appended(0);
        }
        if (fail_consume) {
            return Status::InternalError("share consume failed");
        }
        auto pipe = std::static_pointer_cast<io::StreamLoadPipe>(ctx->body_sink);
        if (batch_kind != BatchKind::ALL_DONE) {
            RETURN_IF_ERROR(pipe->append_with_line_delimiter("row", 3));
        }
        if (fail_pipeline) {
            pipe->cancel("pipeline failed");
            return Status::OK();
        }
        return pipe->finish();
    }

    Status cancel(std::shared_ptr<StreamLoadContext> ctx) override {
        ++cancel_calls;
        return KafkaShareConsumer::cancel(ctx);
    }

protected:
    Status acknowledge_record(int32_t partition, int64_t offset, bool accept) override {
        const bool already_done = batch_kind == BatchKind::ALL_DONE ||
                                  (batch_kind == BatchKind::MIXED_DONE_NEW && partition == 0);
        if (accept && !already_done) {
            EXPECT_FALSE(_ctx.lock()->need_rollback);
        }
        events.emplace_back(accept ? "accept" : "release");
        acknowledgements.emplace_back(partition, offset, accept);
        return Status::OK();
    }

    Status commit_acknowledgements() override {
        ++acknowledgement_commits;
        events.emplace_back("ack");
        return fail_ack ? Status::InternalError("ack response lost") : Status::OK();
    }

private:
    std::weak_ptr<StreamLoadContext> _ctx;
};

class RecordingStreamLoadExecutor : public StreamLoadExecutor {
public:
    RecordingStreamLoadExecutor(ExecEnv* env, std::vector<std::string>* events)
            : StreamLoadExecutor(env), _events(events) {}

    Status commit_result;
    bool lose_rollback_response = false;
    int commit_calls = 0;
    int rollback_calls = 0;
    int rollback_confirmations = 0;

    Status commit_txn(StreamLoadContext* ctx) override {
        ++commit_calls;
        _events->emplace_back("commit");
        if (commit_result.ok() || commit_result.is<ErrorCode::PUBLISH_TIMEOUT>()) {
            ctx->need_rollback = false;
        } else {
            ctx->status = commit_result;
        }
        return commit_result;
    }

    void rollback_txn(StreamLoadContext*) override {
        ++rollback_calls;
        _events->emplace_back(lose_rollback_response ? "rollback_lost" : "rollback");
        if (!lose_rollback_response) {
            ++rollback_confirmations;
        }
    }

private:
    std::vector<std::string>* _events;
};

class RoutineLoadTaskExecutorTest : public testing::Test {
public:
    RoutineLoadTaskExecutorTest() = default;
    ~RoutineLoadTaskExecutorTest() override = default;

    ExecEnv* _env = nullptr;
    std::shared_ptr<TaskShareConsumer> _share_consumer;
    std::shared_ptr<StreamLoadContext> _share_ctx;
    RecordingStreamLoadExecutor* _stream_executor = nullptr;
    size_t _pooled_consumers = 0;

    void run_share_task(TStatusCode::type commit_status) {
        TKafkaLoadInfo info;
        info.__set_use_share_consumer(true);
        _share_ctx = std::make_shared<StreamLoadContext>(_env);
        _share_ctx->id = UniqueId::gen_uid();
        _share_ctx->job_id = 1;
        _share_ctx->db_id = 2;
        _share_ctx->txn_id = 4;
        _share_ctx->auth.token = "source-query-token";
        _share_ctx->load_type = TLoadType::ROUTINE_LOAD;
        _share_ctx->load_src_type = TLoadSourceType::KAFKA;
        _share_ctx->kafka_info = std::make_unique<KafkaLoadInfo>(info);
        _share_ctx->need_rollback = true;
        k_stream_load_commit_result.status.status_code = commit_status;
        if (_stream_executor != nullptr) {
            _stream_executor->commit_result = Status::create(k_stream_load_commit_result.status);
        }
        DataConsumerPool pool;
        pool._pool.push_back(_share_consumer);
        RoutineLoadTaskExecutor executor(_env);
        int finished = 0;
        executor.exec_task(_share_ctx, &pool, [&](auto ctx) { ++finished; });
        if (_share_ctx->load_status_future.valid()) {
            _share_ctx->load_status_future.wait();
        }
        EXPECT_EQ(finished, 1);
        _pooled_consumers = pool._pool.size();
        _env->new_load_stream_mgr()->remove(_share_ctx->id);
        executor.stop();
    }

    void use_recording_executor() {
        auto stream_executor =
                std::make_unique<RecordingStreamLoadExecutor>(_env, &_share_consumer->events);
        _stream_executor = stream_executor.get();
        _env->set_stream_load_executor(std::move(stream_executor));
    }

    std::shared_ptr<StreamLoadContext> completion_context() {
        auto ctx = std::make_shared<StreamLoadContext>(_env);
        ctx->id = UniqueId::gen_uid();
        ctx->db_id = 2;
        ctx->job_id = 1;
        ctx->txn_id = 4;
        ctx->load_type = TLoadType::ROUTINE_LOAD;
        ctx->load_src_type = TLoadSourceType::KAFKA;
        ctx->auth.token = "source-query-token";
        TKafkaLoadInfo info;
        info.__set_use_share_consumer(true);
        ctx->kafka_info = std::make_unique<KafkaLoadInfo>(info);
        return ctx;
    }

    TKafkaSourceDescriptor source_descriptor() {
        TUniqueId topic_id;
        topic_id.__set_hi(7);
        topic_id.__set_lo(9);
        TKafkaSourceOffsetRange range;
        range.__set_first(100);
        range.__set_last(100);
        TKafkaSourcePartition partition;
        partition.__set_topicId(topic_id);
        partition.__set_partition(0);
        partition.__set_ranges({range});
        TKafkaSourceDescriptor source;
        source.__set_namespaceId(1);
        source.__set_clusterId("source-cluster");
        source.__set_partitions({partition});
        return source;
    }

    TRoutineLoadTask guarded_task() {
        TRoutineLoadTask task;
        task.__set_type(TLoadSourceType::KAFKA);
        task.__set_id(UniqueId::gen_uid().to_thrift());
        task.__set_job_id(1);
        task.__set_txn_id(4);
        task.__set_kafka_source_guard_version(KafkaLoadInfo::SOURCE_GUARD_VERSION);
        TKafkaLoadInfo info;
        info.__set_brokers("localhost:9092");
        info.__set_topic("events");
        info.__set_use_share_consumer(true);
        task.__set_kafka_load_info(info);
        return task;
    }

    void SetUp() override {
        _env = ExecEnv::GetInstance();
        k_stream_load_begin_result = TLoadTxnBeginResult();
        k_stream_load_commit_result = TLoadTxnCommitResult();
        k_stream_load_rollback_result = TLoadTxnRollbackResult();
        k_stream_load_put_result = TStreamLoadPutResult();
        k_stream_load_source_completions_result = TGetKafkaSourceCompletionsResult();

        _env->set_cluster_info(new ClusterInfo());
        _env->set_new_load_stream_mgr(NewLoadStreamMgr::create_unique());
        _env->set_stream_load_executor(StreamLoadExecutor::create_unique(_env));

        config::max_routine_load_thread_pool_size = 1024;
        config::max_consumer_num_per_group = 3;
        _share_consumer = std::make_shared<TaskShareConsumer>();
    }

    void TearDown() override {
        delete _env->cluster_info();
        _env->clear_new_load_stream_mgr();
        _env->clear_stream_load_executor();
    }
};

TEST_F(RoutineLoadTaskExecutorTest, ShareAcceptsOnlyAfterCommit) {
    run_share_task(TStatusCode::OK);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 0);
    EXPECT_EQ(_pooled_consumers, 1);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareAcceptsCommittedPublishTimeout) {
    run_share_task(TStatusCode::PUBLISH_TIMEOUT);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 0);
    EXPECT_EQ(_pooled_consumers, 1);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareReleasesAfterCommitFailure) {
    run_share_task(TStatusCode::INTERNAL_ERROR);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, false}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 1);
    EXPECT_EQ(_pooled_consumers, 0);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareReleasesAfterConsumeFailure) {
    _share_consumer->fail_consume = true;
    run_share_task(TStatusCode::OK);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, false}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 1);
    EXPECT_EQ(_pooled_consumers, 0);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareReleasesAfterPipelineFailure) {
    _share_consumer->fail_pipeline = true;
    run_share_task(TStatusCode::OK);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, false}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->cancel_calls, 1);
    EXPECT_EQ(_pooled_consumers, 0);
}

TEST_F(RoutineLoadTaskExecutorTest, ShareAckFailureDoesNotReuseConsumerOrRollback) {
    _share_consumer->fail_ack = true;
    run_share_task(TStatusCode::OK);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, false}}));
    EXPECT_FALSE(_share_ctx->need_rollback);
    EXPECT_EQ(_pooled_consumers, 0);
}

TEST_F(RoutineLoadTaskExecutorTest, AllDoneSkipsCommitAndAbortsUnusedTransactionBeforeAccept) {
    use_recording_executor();
    _share_consumer->batch_kind = TaskShareConsumer::BatchKind::ALL_DONE;
    run_share_task(TStatusCode::INTERNAL_ERROR);
    EXPECT_EQ(_stream_executor->commit_calls, 0);
    EXPECT_EQ(_stream_executor->rollback_calls, 1);
    EXPECT_EQ(_share_ctx->number_loaded_rows, 0);
    EXPECT_FALSE(_share_ctx->need_rollback);
    EXPECT_TRUE(_share_ctx->status.ok());
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, true}}));
    EXPECT_EQ(_share_consumer->events,
              (std::vector<std::string> {"rollback", "accept", "accept", "ack"}));
    EXPECT_EQ(_pooled_consumers, 1);
}

TEST_F(RoutineLoadTaskExecutorTest, AllDonePipelineFailureStillAcceptsDurableRecords) {
    use_recording_executor();
    _share_consumer->batch_kind = TaskShareConsumer::BatchKind::ALL_DONE;
    _share_consumer->fail_pipeline = true;
    run_share_task(TStatusCode::INTERNAL_ERROR);
    EXPECT_EQ(_stream_executor->commit_calls, 0);
    EXPECT_EQ(_stream_executor->rollback_calls, 1);
    EXPECT_FALSE(_share_ctx->need_rollback);
    EXPECT_TRUE(_share_ctx->status.ok());
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, true}}));
    EXPECT_EQ(_share_consumer->acknowledgement_commits, 1);
    EXPECT_EQ(_pooled_consumers, 1);
}

TEST_F(RoutineLoadTaskExecutorTest, AllDoneLostAbortResponseDoesNotPreventAccept) {
    use_recording_executor();
    _share_consumer->batch_kind = TaskShareConsumer::BatchKind::ALL_DONE;
    _stream_executor->lose_rollback_response = true;
    run_share_task(TStatusCode::INTERNAL_ERROR);
    EXPECT_EQ(_stream_executor->commit_calls, 0);
    EXPECT_EQ(_stream_executor->rollback_calls, 1);
    EXPECT_EQ(_stream_executor->rollback_confirmations, 0);
    EXPECT_FALSE(_share_ctx->need_rollback);
    EXPECT_TRUE(_share_ctx->status.ok());
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, true}}));
    EXPECT_EQ(_share_consumer->events,
              (std::vector<std::string> {"rollback_lost", "accept", "accept", "ack"}));
    EXPECT_EQ(_pooled_consumers, 1);
}

TEST_F(RoutineLoadTaskExecutorTest, AllDoneAckFailureFencesConsumerWithoutCommitting) {
    use_recording_executor();
    _share_consumer->batch_kind = TaskShareConsumer::BatchKind::ALL_DONE;
    _share_consumer->fail_ack = true;
    run_share_task(TStatusCode::INTERNAL_ERROR);
    EXPECT_EQ(_stream_executor->commit_calls, 0);
    EXPECT_EQ(_stream_executor->rollback_calls, 1);
    EXPECT_EQ(_share_consumer->acknowledgement_commits, 1);
    EXPECT_FALSE(_share_ctx->need_rollback);
    EXPECT_FALSE(_share_ctx->status.ok());
    EXPECT_FALSE(_share_consumer->reset().ok());
    EXPECT_EQ(_pooled_consumers, 0);
}

TEST_F(RoutineLoadTaskExecutorTest, MixedDoneAndNewCommitAcceptsBothRecords) {
    use_recording_executor();
    _share_consumer->batch_kind = TaskShareConsumer::BatchKind::MIXED_DONE_NEW;
    run_share_task(TStatusCode::OK);
    EXPECT_EQ(_stream_executor->commit_calls, 1);
    EXPECT_EQ(_stream_executor->rollback_calls, 0);
    EXPECT_EQ(_share_ctx->number_loaded_rows, 1);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, true}}));
    EXPECT_EQ(_share_consumer->events,
              (std::vector<std::string> {"commit", "accept", "accept", "ack"}));
    EXPECT_EQ(_pooled_consumers, 1);
}

TEST_F(RoutineLoadTaskExecutorTest, MixedCommitFailureAcceptsDoneAndReleasesNew) {
    use_recording_executor();
    _share_consumer->batch_kind = TaskShareConsumer::BatchKind::MIXED_DONE_NEW;
    run_share_task(TStatusCode::INTERNAL_ERROR);
    EXPECT_EQ(_stream_executor->commit_calls, 1);
    EXPECT_EQ(_stream_executor->rollback_calls, 1);
    EXPECT_EQ(_share_consumer->acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, true}, {1, 50, false}}));
    EXPECT_EQ(_share_consumer->events,
              (std::vector<std::string> {"commit", "rollback", "accept", "release", "ack"}));
    EXPECT_EQ(_pooled_consumers, 0);
}

TEST_F(RoutineLoadTaskExecutorTest, CompletionLookupRejectsInvalidContextAndUnconfirmedResults) {
    auto ctx = completion_context();
    auto source = source_descriptor();
    std::optional<TKafkaSourceDescriptor> completed = source;
    auto* executor = _env->stream_load_executor();
    EXPECT_FALSE(executor->get_kafka_source_completions(ctx.get(), source, &completed).ok());
    EXPECT_FALSE(completed.has_value());

    TStatus failure;
    failure.__set_status_code(TStatusCode::INTERNAL_ERROR);
    k_stream_load_source_completions_result.__set_status(failure);
    completed = source;
    EXPECT_FALSE(executor->get_kafka_source_completions(ctx.get(), source, &completed).ok());
    EXPECT_FALSE(completed.has_value());

    TStatus success;
    success.__set_status_code(TStatusCode::OK);
    k_stream_load_source_completions_result.__set_status(success);
    k_stream_load_source_completions_result.__set_completed_source(source);
    ctx->db_id = 0;
    completed = source;
    EXPECT_FALSE(executor->get_kafka_source_completions(ctx.get(), source, &completed).ok());
    EXPECT_FALSE(completed.has_value());
    ctx->db_id = 2;
    ctx->auth.token.clear();
    completed = source;
    EXPECT_FALSE(executor->get_kafka_source_completions(ctx.get(), source, &completed).ok());
    EXPECT_FALSE(completed.has_value());
}

TEST_F(RoutineLoadTaskExecutorTest, CompletionLookupPreservesEmptyAndPresentSuccessfulEvidence) {
    auto ctx = completion_context();
    auto source = source_descriptor();
    std::optional<TKafkaSourceDescriptor> completed = source;
    TStatus success;
    success.__set_status_code(TStatusCode::OK);
    k_stream_load_source_completions_result.__set_status(success);
    auto* executor = _env->stream_load_executor();
    ASSERT_TRUE(executor->get_kafka_source_completions(ctx.get(), source, &completed).ok());
    EXPECT_FALSE(completed.has_value());

    k_stream_load_source_completions_result.__set_completed_source(source);
    ASSERT_TRUE(executor->get_kafka_source_completions(ctx.get(), source, &completed).ok());
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ(*completed, source);
    completed->partitions[0].ranges[0].__set_last(101);
    EXPECT_EQ(k_stream_load_source_completions_result.completed_source, source);

    ctx->db_id = 0;
    auto& params = ctx->put_result.pipeline_params;
    params.__isset.fragment = true;
    params.fragment.__isset.output_sink = true;
    params.fragment.output_sink.__isset.olap_table_sink = true;
    auto& sink = params.fragment.output_sink.olap_table_sink;
    sink.__set_db_id(2);
    sink.__set_txn_id(ctx->txn_id);
    ASSERT_TRUE(executor->get_kafka_source_completions(ctx.get(), source, &completed).ok());
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ(*completed, source);
    sink.__set_txn_id(ctx->txn_id + 1);
    completed = source;
    EXPECT_FALSE(executor->get_kafka_source_completions(ctx.get(), source, &completed).ok());
    EXPECT_FALSE(completed.has_value());
}

TEST_F(RoutineLoadTaskExecutorTest, GuardVersionRequiresAnExplicitSupportedSubmitRoute) {
    auto task = guarded_task();
    EXPECT_TRUE(RoutineLoadTaskExecutor::validate_task(task).is<ErrorCode::INVALID_ARGUMENT>());
    task.__isset.kafka_source_guard_version = false;
    EXPECT_TRUE(RoutineLoadTaskExecutor::validate_task(task).ok());
    EXPECT_TRUE(RoutineLoadTaskExecutor::validate_task(task, 1).is<ErrorCode::INVALID_ARGUMENT>());
    task.__set_kafka_source_guard_version(2);
    EXPECT_TRUE(RoutineLoadTaskExecutor::validate_task(task, 1).is<ErrorCode::INVALID_ARGUMENT>());
    task.__set_kafka_source_guard_version(-1);
    EXPECT_TRUE(RoutineLoadTaskExecutor::validate_task(task).is<ErrorCode::INVALID_ARGUMENT>());

    const bool previous = config::enable_kafka_share_completion_filtering;
    Defer restore {[&] { config::enable_kafka_share_completion_filtering = previous; }};
    config::enable_kafka_share_completion_filtering = false;
    task.__set_kafka_source_guard_version(1);
#ifdef HAVE_RDKAFKA_MESSAGE_TOPIC_ID
    EXPECT_TRUE(RoutineLoadTaskExecutor::validate_task(task, 1).is<ErrorCode::INVALID_ARGUMENT>());
#else
    EXPECT_TRUE(
            RoutineLoadTaskExecutor::validate_task(task, 1).is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
#endif
}

#ifdef HAVE_RDKAFKA_MESSAGE_TOPIC_ID
TEST_F(RoutineLoadTaskExecutorTest,
       GuardedResubmissionValidatesModeAndOwnerBeforeDuplicateShortcut) {
    const bool previous = config::enable_kafka_share_completion_filtering;
    const std::string previous_mode = config::deploy_mode;
    const std::string previous_cloud = config::cloud_unique_id;
    Defer restore {[&] {
        config::enable_kafka_share_completion_filtering = previous;
        config::deploy_mode = previous_mode;
        config::cloud_unique_id = previous_cloud;
    }};
    config::enable_kafka_share_completion_filtering = true;
    config::deploy_mode = "local";
    config::cloud_unique_id.clear();
    auto task = guarded_task();
    auto ctx = completion_context();
    ctx->id = UniqueId(task.id);
    RoutineLoadTaskExecutor executor(_env);
    Defer stop {[&] { executor.stop(); }};
    executor._task_map[ctx->id] = ctx;
    EXPECT_TRUE(executor.submit_task(task, 1).is<ErrorCode::INVALID_ARGUMENT>());
    ctx->kafka_info->source_guard_version = 1;
    EXPECT_TRUE(executor.submit_task(task, 1).ok());
    task.__set_txn_id(5);
    EXPECT_TRUE(executor.submit_task(task, 1).is<ErrorCode::INVALID_ARGUMENT>());
    task.__set_txn_id(4);
    task.__set_job_id(2);
    EXPECT_TRUE(executor.submit_task(task, 1).is<ErrorCode::INVALID_ARGUMENT>());
    task.__set_job_id(1);
    task.__isset.kafka_source_guard_version = false;
    EXPECT_TRUE(executor.submit_task(task).is<ErrorCode::INVALID_ARGUMENT>());
    task.__set_kafka_source_guard_version(1);
    config::enable_kafka_share_completion_filtering = false;
    EXPECT_TRUE(executor.submit_task(task, 1).is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(executor._task_map.size(), 1);
    EXPECT_EQ(executor._task_map.at(ctx->id), ctx);
}
#endif

TEST_F(RoutineLoadTaskExecutorTest, GuardProofComesFromTaskContextAndSurvivesProofOnlyRollback) {
    const bool previous = config::enable_kafka_share_completion_filtering;
    Defer restore {[&] { config::enable_kafka_share_completion_filtering = previous; }};
    config::enable_kafka_share_completion_filtering = true;
    auto ctx = completion_context();
    auto* executor = _env->stream_load_executor();
    TLoadTxnCommitRequest legacy;
    executor->get_commit_request(ctx.get(), legacy);
    EXPECT_FALSE(legacy.txnCommitAttachment.rlTaskTxnCommitAttachment.__isset
                         .kafka_source_guard_version);

    ctx->kafka_info->source_guard_version = 1;
    ctx->kafka_info->source_descriptor = source_descriptor();
    TLoadTxnCommitRequest guarded;
    executor->get_commit_request(ctx.get(), guarded);
    const auto& attachment = guarded.txnCommitAttachment.rlTaskTxnCommitAttachment;
    ASSERT_TRUE(attachment.__isset.kafka_source_guard_version);
    EXPECT_EQ(attachment.kafka_source_guard_version, 1);
    EXPECT_EQ(attachment.kafkaSourceDescriptor, *ctx->kafka_info->source_descriptor);

    ctx->kafka_info->source_descriptor.reset();
    TTxnCommitAttachment rollback;
    ASSERT_TRUE(executor->collect_load_stat(ctx.get(), &rollback));
    EXPECT_TRUE(rollback.rlTaskTxnCommitAttachment.__isset.kafka_source_guard_version);
    EXPECT_EQ(rollback.rlTaskTxnCommitAttachment.kafka_source_guard_version, 1);
    EXPECT_FALSE(rollback.rlTaskTxnCommitAttachment.__isset.kafkaSourceDescriptor);
}

TEST_F(RoutineLoadTaskExecutorTest, exec_task) {
    TRoutineLoadTask task;
    task.type = TLoadSourceType::KAFKA;
    task.job_id = 1L;
    task.id = TUniqueId();
    task.txn_id = 4;
    task.auth_code = 5;
    task.__set_db("db1");
    task.__set_tbl("tbl1");
    task.__set_label("l1");
    task.__set_max_interval_s(5);
    task.__set_max_batch_rows(10);
    task.__set_max_batch_size(2048);

    TKafkaLoadInfo k_info;
    k_info.brokers = "127.0.0.1:9092";
    k_info.topic = "test";

    std::map<int32_t, int64_t> part_off;
    part_off[0] = 13L;
    k_info.__set_partition_begin_offset(part_off);

    task.__set_kafka_load_info(k_info);

    RoutineLoadTaskExecutor executor(_env);
    Status st;
    st = executor.init(1024 * 1024);
    EXPECT_TRUE(st.ok());
    // submit task
    st = executor.submit_task(task);
    EXPECT_TRUE(st.ok());

    usleep(200);
    k_info.brokers = "127.0.0.1:9092";
    task.__set_kafka_load_info(k_info);
    st = executor.submit_task(task);
    EXPECT_TRUE(st.ok());

    usleep(200);
    k_info.brokers = "192.0.0.2:9092";
    task.__set_kafka_load_info(k_info);
    st = executor.submit_task(task);
    EXPECT_TRUE(st.ok());

    usleep(200);
    k_info.brokers = "192.0.0.2:9092";
    task.__set_kafka_load_info(k_info);
    st = executor.submit_task(task);
    EXPECT_TRUE(st.ok());

    executor.stop();
}

} // namespace doris
