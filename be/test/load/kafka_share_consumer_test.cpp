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

#include "load/routine_load/kafka_share_consumer.h"

#include <gtest/gtest.h>

#include <cstring>
#include <functional>
#include <initializer_list>
#include <limits>
#include <thread>
#include <tuple>
#include <vector>

#include "common/env_config.h"
#include "io/fs/stream_load_pipe.h"
#include "load/routine_load/kafka_reader_telemetry.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"

namespace doris {

namespace {
void prepare_batch(KafkaShareBatch& batch, std::initializer_list<KafkaShareBatch::Record> records,
                   size_t processed) {
    batch.reserve(records.size());
    for (const auto& record : records) {
        batch.add_record(record.partition, record.offset);
    }
    for (size_t i = 0; i < processed; ++i) {
        batch.mark_appended(i);
    }
}

std::optional<TKafkaSourceDescriptor> build_descriptor(
        KafkaShareConsumer& consumer, int64_t namespace_id, const std::string& cluster_id,
        std::initializer_list<std::pair<uint64_t, uint64_t>> topic_ids) {
    size_t index = 0;
    for (const auto& [hi, lo] : topic_ids) {
        consumer._batch.set_topic_id(index++, hi, lo);
    }
    return consumer.build_source_descriptor(namespace_id, cluster_id, true);
}
} // namespace

class RecordingShareConsumer : public KafkaShareConsumer {
public:
    std::vector<std::tuple<int32_t, int64_t, bool>> acknowledgements;
    bool fail_record = false;
    int64_t fail_offset = -1;
    bool fail_commit = false;
    int commits = 0;

protected:
    Status acknowledge_record(int32_t partition, int64_t offset, bool accept) override {
        if (fail_record || offset == fail_offset) {
            return Status::InternalError("expired acquisition");
        }
        acknowledgements.emplace_back(partition, offset, accept);
        return Status::OK();
    }
    Status commit_acknowledgements() override {
        ++commits;
        return fail_commit ? Status::InternalError("lost acknowledgement response") : Status::OK();
    }
};

class DeliveredShareConsumer : public RecordingShareConsumer {
public:
    Status identity_status;
    std::string cluster_id = "cluster-a";
    std::vector<std::pair<uint64_t, uint64_t>> topic_ids;

protected:
    Status capture_source_identity(std::span<rd_kafka_message_t* const> messages,
                                   bool appended_only, std::string* cluster) override {
        RETURN_IF_ERROR(identity_status);
        *cluster = cluster_id;
        for (size_t i = 0; i < messages.size(); ++i) {
            if (!appended_only ||
                _batch.record(i).disposition == KafkaShareBatch::Disposition::APPENDED) {
                auto [hi, lo] =
                        topic_ids.empty() ? std::pair<uint64_t, uint64_t>(1, 2) : topic_ids[i];
                _batch.set_topic_id(i, hi, lo);
            }
        }
        return Status::OK();
    }
};

class RecordingSharePipe : public io::StreamLoadPipe {
public:
    std::vector<std::string> rows;
    int fail_after = -1;
    bool finished = false;

    Status append_with_line_delimiter(const char* data, size_t size) override {
        if (std::cmp_equal(rows.size(), fail_after)) {
            return Status::InternalError("append failed");
        }
        rows.emplace_back(data, size);
        return Status::OK();
    }

    Status finish() override {
        finished = true;
        return Status::OK();
    }
};

class CompletionLookupExecutor : public StreamLoadExecutor {
public:
    explicit CompletionLookupExecutor(ExecEnv* env) : StreamLoadExecutor(env) {}

    Status lookup_status;
    int calls = 0;
    std::vector<TKafkaSourceDescriptor> queries;
    std::function<std::optional<TKafkaSourceDescriptor>(const TKafkaSourceDescriptor&)> response;

    Status get_kafka_source_completions(StreamLoadContext*, const TKafkaSourceDescriptor& source,
                                        std::optional<TKafkaSourceDescriptor>* completed) override {
        ++calls;
        queries.push_back(source);
        RETURN_IF_ERROR(lookup_status);
        *completed = response ? response(source) : std::nullopt;
        return Status::OK();
    }
};

class KafkaShareFilteringTest : public testing::Test {
public:
    void SetUp() override {
        env = ExecEnv::GetInstance();
        auto lookup = std::make_unique<CompletionLookupExecutor>(env);
        executor = lookup.get();
        env->set_stream_load_executor(std::move(lookup));
        ctx = std::make_shared<StreamLoadContext>(env);
        ctx->job_id = 42;
        TKafkaLoadInfo info;
        info.__set_use_share_consumer(true);
        ctx->kafka_info = std::make_unique<KafkaLoadInfo>(info);
        pipe = std::make_shared<RecordingSharePipe>();
        ctx->body_sink = pipe;
    }

    void TearDown() override {
        ctx.reset();
        env->clear_stream_load_executor();
    }

    static rd_kafka_message_t message(int32_t partition, int64_t offset,
                                      const char* payload = "row") {
        rd_kafka_message_t result {};
        result.partition = partition;
        result.offset = offset;
        result.payload = const_cast<char*>(payload);
        result.len = payload == nullptr ? 0 : std::strlen(payload);
        return result;
    }

    Status consume(DeliveredShareConsumer& consumer, std::vector<rd_kafka_message_t>& messages,
                   bool filtering = true) {
        std::vector<rd_kafka_message_t*> pointers;
        for (auto& message : messages) {
            pointers.push_back(&message);
        }
        return consumer.consume_messages(ctx, pointers, filtering);
    }

    static TKafkaSourceDescriptor with_ranges(
            const TKafkaSourceDescriptor& query,
            std::initializer_list<std::pair<int64_t, int64_t>> ranges) {
        auto result = query;
        result.partitions[0].__set_ranges({});
        for (const auto& [first, last] : ranges) {
            TKafkaSourceOffsetRange range;
            range.__set_first(first);
            range.__set_last(last);
            result.partitions[0].ranges.push_back(range);
        }
        return result;
    }

    ExecEnv* env = nullptr;
    CompletionLookupExecutor* executor = nullptr;
    std::shared_ptr<StreamLoadContext> ctx;
    std::shared_ptr<RecordingSharePipe> pipe;
};

TEST_F(KafkaShareFilteringTest, MixedCompletionKeepsSparseAppendedIdentityAndAbortDecisions) {
    DeliveredShareConsumer consumer;
    auto messages = std::vector {message(0, 9), message(0, 4), message(0, 2),
                                 message(0, 4), message(0, 7), message(0, 8)};
    executor->response = [](const auto& query) { return with_ranges(query, {{9, 9}}); };
    ctx->max_batch_rows = 2;
    ASSERT_TRUE(consume(consumer, messages).ok());
    EXPECT_EQ(pipe->rows.size(), 2);
    EXPECT_EQ(consumer._batch.processed(), 2);
    EXPECT_EQ(consumer._batch.mem_tracker()->consumption(),
              consumer._batch.capacity() * sizeof(KafkaShareBatch::Record));
    EXPECT_FALSE(consumer.all_records_completed());
    ASSERT_TRUE(ctx->kafka_info->source_descriptor.has_value());
    const auto& ranges = ctx->kafka_info->source_descriptor->partitions[0].ranges;
    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges[0].first, 2);
    EXPECT_EQ(ranges[0].last, 2);
    EXPECT_EQ(ranges[1].first, 4);
    EXPECT_EQ(ranges[1].last, 4);
    ASSERT_TRUE(consumer.cancel(ctx).ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 9, true},
                                                                {0, 4, false},
                                                                {0, 2, false},
                                                                {0, 4, false},
                                                                {0, 7, false},
                                                                {0, 8, false}}));
}

TEST_F(KafkaShareFilteringTest, CommitAcceptsRepresentativesAndAliasesButReleasesUntouched) {
    DeliveredShareConsumer consumer;
    auto messages =
            std::vector {message(0, 9), message(0, 4), message(0, 4), message(0, 2), message(0, 9)};
    executor->response = [](const auto& query) { return with_ranges(query, {{9, 9}}); };
    ctx->max_batch_rows = 1;
    ASSERT_TRUE(consume(consumer, messages).ok());
    EXPECT_EQ(pipe->rows.size(), 1);
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {
                      {0, 9, true}, {0, 4, true}, {0, 4, true}, {0, 2, false}, {0, 9, true}}));
}

TEST_F(KafkaShareFilteringTest, AllDoneIncludesRecordsBeyondLimitsAndHasNoCommitDescriptor) {
    DeliveredShareConsumer consumer;
    auto messages = std::vector {message(0, 9, nullptr), message(1, 4), message(0, 9, nullptr)};
    executor->response = [](const auto& query) { return query; };
    ctx->max_batch_rows = 0;
    ASSERT_TRUE(consume(consumer, messages).ok());
    EXPECT_TRUE(consumer.all_records_completed());
    EXPECT_EQ(consumer._batch.processed(), 0);
    EXPECT_TRUE(pipe->rows.empty());
    EXPECT_TRUE(pipe->finished);
    EXPECT_FALSE(ctx->kafka_info->source_descriptor.has_value());
    ASSERT_TRUE(consumer.cancel(ctx).ok());
    EXPECT_EQ(consumer.acknowledgements, (std::vector<std::tuple<int32_t, int64_t, bool>> {
                                                 {0, 9, true}, {1, 4, true}, {0, 9, true}}));
    EXPECT_FALSE(consumer.all_records_completed());
}

TEST_F(KafkaShareFilteringTest, MissingEvidenceAppendsUnknownInputsOnce) {
    auto messages = std::vector {message(0, 9), message(0, 4), message(0, 4)};
    DeliveredShareConsumer consumer;
    ASSERT_TRUE(consume(consumer, messages).ok());
    EXPECT_EQ(consumer._batch.processed(), 2);
    EXPECT_FALSE(consumer.all_records_completed());
    ASSERT_TRUE(consumer.cancel(ctx).ok());
    for (const auto& [partition, offset, accept] : consumer.acknowledgements) {
        EXPECT_FALSE(accept);
    }
    EXPECT_EQ(pipe->rows.size(), 2);
}

TEST_F(KafkaShareFilteringTest, AppendFailureKeepsDoneAndReleasesAppendedAliasAndSuffix) {
    DeliveredShareConsumer consumer;
    auto messages =
            std::vector {message(0, 9), message(0, 4), message(0, 4), message(0, 2), message(0, 7)};
    executor->response = [](const auto& query) { return with_ranges(query, {{9, 9}}); };
    pipe->fail_after = 1;
    EXPECT_FALSE(consume(consumer, messages).ok());
    EXPECT_EQ(pipe->rows.size(), 1);
    ASSERT_TRUE(consumer.cancel(ctx).ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {
                      {0, 9, true}, {0, 4, false}, {0, 4, false}, {0, 2, false}, {0, 7, false}}));
}

TEST_F(KafkaShareFilteringTest, IdentityLookupAndRecordErrorsFailBeforeAppend) {
    for (int failure = 0; failure < 6; ++failure) {
        DeliveredShareConsumer consumer;
        auto messages = std::vector {message(0, 9), message(0, 4)};
        executor->lookup_status = Status::OK();
        if (failure == 0) {
            consumer.identity_status = Status::InternalError("unavailable identity");
        } else if (failure == 1) {
            consumer.cluster_id.clear();
        } else if (failure == 2) {
            consumer.topic_ids = {{1, 2}, {0, 0}};
        } else if (failure == 3) {
            consumer.topic_ids = {{1, 2}, {1, 3}};
        } else if (failure == 4) {
            executor->lookup_status = Status::InternalError("lookup failed");
        } else {
            messages[1].err = RD_KAFKA_RESP_ERR__BAD_MSG;
        }
        EXPECT_FALSE(consume(consumer, messages).ok()) << failure;
        EXPECT_TRUE(pipe->rows.empty());
        ASSERT_TRUE(consumer.cancel(ctx).ok());
        for (const auto& [partition, offset, accept] : consumer.acknowledgements) {
            EXPECT_FALSE(accept) << failure;
        }
    }
}

TEST_F(KafkaShareFilteringTest, LookupPublishTimeoutIsFatalBeforeAppend) {
    DeliveredShareConsumer consumer;
    auto messages = std::vector {message(0, 4)};
    executor->lookup_status = Status::PublishTimeout("completion lookup timed out");
    auto status = consume(consumer, messages);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.is<ErrorCode::PUBLISH_TIMEOUT>());
    EXPECT_TRUE(pipe->rows.empty());
    ASSERT_TRUE(consumer.cancel(ctx).ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 4, false}}));
}

TEST_F(KafkaShareFilteringTest, MalformedOrNonSubsetReplyNeverPartiallyMarksDone) {
    using Mutation = std::function<void(TKafkaSourceDescriptor&)>;
    const auto mutations = std::vector<Mutation> {
            [](auto& value) { value.namespaceId++; },
            [](auto& value) { value.clusterId = "another-cluster"; },
            [](auto& value) { value.__isset.partitions = false; },
            [](auto& value) { value.__set_partitions({}); },
            [](auto& value) { value.partitions[0].__isset.topicId = false; },
            [](auto& value) { value.partitions[0].topicId.lo++; },
            [](auto& value) { value.partitions[0].partition = -1; },
            [](auto& value) { value.partitions[0].__isset.ranges = false; },
            [](auto& value) {
                value.partitions[0].ranges.resize(1);
                value.partitions[0].ranges[0].last = 4;
            },
            [](auto& value) { value.partitions[0].ranges[1].last = 8; },
            [](auto& value) { value.partitions[0].ranges[1].__isset.last = false; },
            [](auto& value) { value.partitions[0].ranges[1].first = -1; },
            [](auto& value) { value.partitions[0].ranges[1].last = 3; },
            [](auto& value) { value.partitions[0].ranges[1].first = 3; },
            [](auto& value) { value.partitions.push_back(value.partitions[0]); },
    };
    for (const auto& mutate : mutations) {
        DeliveredShareConsumer consumer;
        auto messages = std::vector {message(0, 2), message(0, 4)};
        executor->response = [&](const auto& query) {
            auto result = query;
            mutate(result);
            return result;
        };
        EXPECT_FALSE(consume(consumer, messages).ok());
        EXPECT_TRUE(pipe->rows.empty());
        ASSERT_TRUE(consumer.cancel(ctx).ok());
        EXPECT_EQ(consumer.acknowledgements,
                  (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 2, false}, {0, 4, false}}));
    }
}

TEST_F(KafkaShareFilteringTest, InconsistentDuplicateAndUnknownNullFailClosed) {
    DeliveredShareConsumer consumer;
    auto inconsistent = std::vector {message(0, 4, "a"), message(0, 4, "b")};
    EXPECT_FALSE(consume(consumer, inconsistent).ok());
    EXPECT_EQ(executor->calls, 0);
    EXPECT_TRUE(pipe->rows.empty());
    ASSERT_TRUE(consumer.cancel(ctx).ok());
    auto unknown_null = std::vector {message(0, 4, nullptr)};
    EXPECT_FALSE(consume(consumer, unknown_null).ok());
    EXPECT_FALSE(consumer.all_records_completed());
    EXPECT_TRUE(pipe->rows.empty());
    ASSERT_TRUE(consumer.cancel(ctx).ok());
    for (const auto& [partition, offset, accept] : consumer.acknowledgements) {
        EXPECT_FALSE(accept);
    }
}

TEST_F(KafkaShareFilteringTest, DisabledPathKeepsDuplicatesAndNullWithoutLookup) {
    DeliveredShareConsumer consumer;
    consumer.identity_status = Status::InternalError("old client");
    auto messages = std::vector {message(0, 4), message(0, 4), message(0, 5, nullptr)};
    ASSERT_TRUE(consume(consumer, messages, false).ok());
    EXPECT_EQ(executor->calls, 0);
    EXPECT_EQ(pipe->rows.size(), 2);
    EXPECT_EQ(consumer._batch.processed(), 3);
    EXPECT_FALSE(consumer.all_records_completed());
    EXPECT_FALSE(ctx->kafka_info->source_descriptor.has_value());
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    for (const auto& [partition, offset, accept] : consumer.acknowledgements) {
        EXPECT_TRUE(accept);
    }
}

TEST_F(KafkaShareFilteringTest, EmptyPollIsNotAllDoneAndDoesNotLookup) {
    DeliveredShareConsumer consumer;
    std::vector<rd_kafka_message_t> messages;
    ASSERT_TRUE(consume(consumer, messages).ok());
    EXPECT_FALSE(consumer.all_records_completed());
    EXPECT_EQ(executor->calls, 0);
    EXPECT_TRUE(consumer.cancel(ctx).ok());
    EXPECT_EQ(consumer.commits, 0);
}

TEST_F(KafkaShareFilteringTest, CompletionPreservesUnsignedTopicOrderingAndMaximumOffset) {
    DeliveredShareConsumer consumer;
    constexpr auto max_offset = std::numeric_limits<int64_t>::max();
    constexpr auto high_bit = uint64_t {1} << 63;
    consumer.topic_ids = {{high_bit, 4}, {1, 4}, {high_bit, 4}, {1, 4}};
    auto messages = std::vector {message(0, max_offset), message(1, 7), message(0, max_offset - 1),
                                 message(1, 9)};
    executor->response = [](const auto& query) { return query; };
    ASSERT_TRUE(consume(consumer, messages).ok());
    ASSERT_EQ(executor->queries.size(), 1);
    const auto& query = executor->queries[0];
    ASSERT_EQ(query.partitions.size(), 2);
    EXPECT_EQ(query.partitions[0].topicId.hi, 1);
    EXPECT_EQ(query.partitions[1].topicId.hi, std::numeric_limits<int64_t>::min());
    EXPECT_EQ(query.partitions[1].ranges[0].first, max_offset - 1);
    EXPECT_EQ(query.partitions[1].ranges[0].last, max_offset);
    EXPECT_TRUE(consumer.all_records_completed());
    EXPECT_TRUE(pipe->rows.empty());
    ASSERT_TRUE(consumer.cancel(ctx).ok());
    for (const auto& [partition, offset, accept] : consumer.acknowledgements) {
        EXPECT_TRUE(accept);
    }
}

TEST(KafkaShareConsumerTest, CommitAcceptsOnlyWrittenRecords) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 101}, {0, 100}, {1, 50}}, 2);
    EXPECT_FALSE(consumer.reset().ok());
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.acknowledgements, (std::vector<std::tuple<int32_t, int64_t, bool>> {
                                                 {0, 101, true}, {0, 100, true}, {1, 50, false}}));
    EXPECT_EQ(consumer.commits, 1);
    EXPECT_TRUE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, AbortReleasesEveryRecord) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 100}, {0, 101}}, 1);
    ASSERT_TRUE(consumer.cancel(nullptr).ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, false}, {0, 101, false}}));
    EXPECT_TRUE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, Completing101DoesNotAccept100) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 101}, {0, 100}}, 1);
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 101, true}, {0, 100, false}}));
}

TEST(KafkaShareConsumerTest, FailedAcknowledgementCannotBeReused) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 100}}, 1);
    consumer.fail_commit = true;
    EXPECT_FALSE(consumer.acknowledge_committed().ok());
    EXPECT_FALSE(consumer.reset().ok());
    EXPECT_TRUE(consumer._batch.empty());
}

TEST(KafkaShareConsumerTest, ExpiredAcquisitionCannotBeReused) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 100}}, 1);
    consumer.fail_record = true;
    EXPECT_FALSE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.commits, 0);
    EXPECT_FALSE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, ExpiredAcquisitionDoesNotStrandCommittedRecords) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 100}, {1, 50}, {2, 75}}, 2);
    consumer.fail_offset = 100;
    EXPECT_FALSE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{1, 50, true}, {2, 75, false}}));
    EXPECT_EQ(consumer.commits, 1);
    EXPECT_FALSE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, ExpiredAcquisitionDoesNotStrandReleasedRecords) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 100}, {1, 50}, {2, 75}}, 2);
    consumer.fail_offset = 100;
    EXPECT_FALSE(consumer.cancel(nullptr).ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{1, 50, false}, {2, 75, false}}));
    EXPECT_EQ(consumer.commits, 1);
    EXPECT_FALSE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, EmptyBatchDoesNotCommitAcknowledgements) {
    RecordingShareConsumer consumer;
    EXPECT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_TRUE(consumer.cancel(nullptr).ok());
    EXPECT_EQ(consumer.commits, 0);
    EXPECT_TRUE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, ReuseKeepsCapacityAndResetsProcessedPrefix) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 101}, {0, 100}, {1, 50}}, 2);
    auto capacity = consumer._batch.capacity();
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer._batch.capacity(), capacity);
    EXPECT_EQ(consumer._batch.processed(), 0);
    ASSERT_TRUE(consumer.reset().ok());

    prepare_batch(consumer._batch, {{2, 9}, {2, 4}}, 1);
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(
            consumer.acknowledgements,
            (std::vector<std::tuple<int32_t, int64_t, bool>> {
                    {0, 101, true}, {0, 100, true}, {1, 50, false}, {2, 9, true}, {2, 4, false}}));
    EXPECT_EQ(consumer._batch.capacity(), capacity);
    EXPECT_EQ(consumer.commits, 2);
    EXPECT_TRUE(consumer._batch.empty());
    EXPECT_TRUE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, BufferMemoryFollowsConsumerAcrossThreads) {
    auto consumer = std::make_unique<RecordingShareConsumer>();
    auto tracker = consumer->_batch.mem_tracker();
    std::thread worker([&] {
        consumer->_batch.reserve(1024);
        EXPECT_EQ(tracker->consumption(),
                  consumer->_batch.capacity() * sizeof(KafkaShareBatch::Record));
        consumer->_batch.add_record(0, 9);
        consumer->_batch.add_record(0, 4);
        consumer->_batch.add_record(0, 4);
        consumer->_batch.set_topic_id(0, 1, 2);
        consumer->_batch.mark_done(0);
        consumer->_batch.mark_appended(1);
        consumer->_batch.mark_duplicate(2, 1);
        EXPECT_EQ(consumer->_batch.record(0).topic_id_lo, 2);
        EXPECT_TRUE(consumer->_batch.should_accept(0, false));
        EXPECT_FALSE(consumer->_batch.should_accept(2, false));
        EXPECT_TRUE(consumer->_batch.should_accept(2, true));
        EXPECT_TRUE(consumer->acknowledge_committed().ok());
        EXPECT_TRUE(consumer->reset().ok());
        consumer->_batch.reserve(4096);
        EXPECT_EQ(tracker->consumption(),
                  consumer->_batch.capacity() * sizeof(KafkaShareBatch::Record));
    });
    worker.join();
    EXPECT_GT(tracker->consumption(), 0);
    auto caller_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "ShareTestCaller");
    std::thread cleanup([&] {
        SCOPED_ATTACH_TASK(caller_tracker);
        consumer->_batch.reserve(8192);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                  caller_tracker.get());
        consumer.reset();
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                  caller_tracker.get());
    });
    cleanup.join();
    EXPECT_EQ(tracker->consumption(), 0);
    EXPECT_EQ(caller_tracker->consumption(), 0);
}

TEST(KafkaShareConsumerTest, SourceDescriptorKeepsProcessedPrefixAndSparseOffsets) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{1, 9}, {0, 4}, {0, 2}, {0, 3}, {0, 6}, {0, 5}}, 5);
    auto descriptor = build_descriptor(consumer, 42, "cluster-a",
                                       {{1, 2}, {1, 2}, {1, 2}, {1, 2}, {1, 2}, {0, 0}});
    ASSERT_TRUE(descriptor.has_value());
    EXPECT_EQ(descriptor->namespaceId, 42);
    EXPECT_EQ(descriptor->clusterId, "cluster-a");
    ASSERT_EQ(descriptor->partitions.size(), 2);
    const auto& partition = descriptor->partitions[0];
    EXPECT_EQ(partition.topicId.hi, 1);
    EXPECT_EQ(partition.topicId.lo, 2);
    EXPECT_EQ(partition.partition, 0);
    ASSERT_EQ(partition.ranges.size(), 2);
    EXPECT_EQ(partition.ranges[0].first, 2);
    EXPECT_EQ(partition.ranges[0].last, 4);
    EXPECT_EQ(partition.ranges[1].first, 6);
    EXPECT_EQ(partition.ranges[1].last, 6);
    EXPECT_EQ(descriptor->partitions[1].partition, 1);
    ASSERT_EQ(descriptor->partitions[1].ranges.size(), 1);
    EXPECT_EQ(descriptor->partitions[1].ranges[0].first, 9);
    EXPECT_EQ(descriptor->partitions[1].ranges[0].last, 9);
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{1, 9, true},
                                                                {0, 4, true},
                                                                {0, 2, true},
                                                                {0, 3, true},
                                                                {0, 6, true},
                                                                {0, 5, false}}));
}

TEST(KafkaShareConsumerTest, SourceDescriptorSeparatesTopicIdsAndHandlesMaximumOffset) {
    RecordingShareConsumer consumer;
    constexpr auto max_offset = std::numeric_limits<int64_t>::max();
    constexpr uint64_t high_bit = uint64_t {1} << 63;
    prepare_batch(consumer._batch,
                  {{0, max_offset}, {0, max_offset - 1}, {0, max_offset}, {0, 7}, {0, 8}}, 5);
    auto descriptor =
            build_descriptor(consumer, 42, "cluster-a",
                             {{high_bit, 4}, {high_bit, 4}, {high_bit, 4}, {1, 4}, {1, 4}});
    ASSERT_TRUE(descriptor.has_value());
    ASSERT_EQ(descriptor->partitions.size(), 2);
    EXPECT_EQ(descriptor->partitions[0].topicId.hi, 1);
    ASSERT_EQ(descriptor->partitions[0].ranges.size(), 1);
    EXPECT_EQ(descriptor->partitions[0].ranges[0].first, 7);
    EXPECT_EQ(descriptor->partitions[0].ranges[0].last, 8);
    EXPECT_EQ(descriptor->partitions[1].topicId.hi, std::numeric_limits<int64_t>::min());
    EXPECT_EQ(descriptor->partitions[1].topicId.lo, 4);
    ASSERT_EQ(descriptor->partitions[1].ranges.size(), 1);
    EXPECT_EQ(descriptor->partitions[1].ranges[0].first, max_offset - 1);
    EXPECT_EQ(descriptor->partitions[1].ranges[0].last, max_offset);
}

TEST(KafkaShareConsumerTest, MissingMetadataOmitsWholeSourceDescriptor) {
    RecordingShareConsumer consumer;
    EXPECT_FALSE(build_descriptor(consumer, 42, "cluster-a", {}).has_value());
    prepare_batch(consumer._batch, {{0, 2}, {0, 4}}, 2);
    EXPECT_FALSE(build_descriptor(consumer, 42, "", {{1, 2}, {1, 2}}).has_value());
    EXPECT_FALSE(build_descriptor(consumer, 42, "cluster-a", {{1, 2}, {0, 0}}).has_value());
}

TEST(KafkaShareConsumerTest, SourceDescriptorReachesNativeCommitAttachment) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 2}, {0, 4}}, 1);
    StreamLoadContext ctx(nullptr);
    ctx.job_id = 42;
    ctx.load_type = TLoadType::ROUTINE_LOAD;
    ctx.load_src_type = TLoadSourceType::KAFKA;
    TKafkaLoadInfo info;
    info.__set_use_share_consumer(true);
    ctx.kafka_info = std::make_unique<KafkaLoadInfo>(info);
    ctx.kafka_info->source_descriptor =
            build_descriptor(consumer, ctx.job_id, "cluster-a", {{1, 2}, {1, 2}});
    ASSERT_TRUE(ctx.kafka_info->source_descriptor.has_value());
    StreamLoadExecutor executor(nullptr);
    TLoadTxnCommitRequest request;
    executor.get_commit_request(&ctx, request);
    ASSERT_TRUE(request.__isset.txnCommitAttachment);
    ASSERT_TRUE(request.txnCommitAttachment.__isset.rlTaskTxnCommitAttachment);
    const auto& attachment = request.txnCommitAttachment.rlTaskTxnCommitAttachment;
    ASSERT_TRUE(attachment.__isset.kafkaSourceDescriptor);
    EXPECT_EQ(attachment.kafkaSourceDescriptor, *ctx.kafka_info->source_descriptor);

    TTxnCommitAttachment rollback_attachment;
    ASSERT_TRUE(executor.collect_load_stat(&ctx, &rollback_attachment));
    EXPECT_EQ(rollback_attachment.rlTaskTxnCommitAttachment.kafkaSourceDescriptor,
              attachment.kafkaSourceDescriptor);
    ctx.kafka_info->source_descriptor.reset();
    TTxnCommitAttachment legacy_attachment;
    ASSERT_TRUE(executor.collect_load_stat(&ctx, &legacy_attachment));
    EXPECT_FALSE(legacy_attachment.rlTaskTxnCommitAttachment.__isset.kafkaSourceDescriptor);
}

#ifndef HAVE_RDKAFKA_MESSAGE_TOPIC_ID
TEST(KafkaShareConsumerTest, UnavailableIdentityApiPreservesAcknowledgements) {
    RecordingShareConsumer consumer;
    prepare_batch(consumer._batch, {{0, 2}, {0, 4}}, 1);
    StreamLoadContext ctx(nullptr);
    TKafkaLoadInfo info;
    ctx.kafka_info = std::make_unique<KafkaLoadInfo>(info);
    ctx.kafka_info->source_descriptor = TKafkaSourceDescriptor();
    std::string cluster_id;
    EXPECT_FALSE(consumer.capture_source_identity({}, true, &cluster_id).ok());
    EXPECT_FALSE(consumer.build_source_descriptor(42, cluster_id, true).has_value());
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 2, true}, {0, 4, false}}));
    EXPECT_TRUE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, UnavailableIdentityApiRejectsFilteringBeforeAcquisition) {
    const bool previous = config::enable_kafka_share_completion_filtering;
    config::enable_kafka_share_completion_filtering = true;
    RecordingShareConsumer consumer;
    auto ctx = std::make_shared<StreamLoadContext>(nullptr);
    TKafkaLoadInfo info;
    ctx->kafka_info = std::make_unique<KafkaLoadInfo>(info);
    EXPECT_FALSE(consumer.init(ctx).ok());
    EXPECT_FALSE(consumer.consume(ctx).ok());
    EXPECT_TRUE(consumer._batch.empty());
    config::enable_kafka_share_completion_filtering = previous;
}
#endif

TEST(KafkaReaderTelemetryTest, DisabledDoesNotRetainRecords) {
    KafkaReaderTelemetry telemetry(42, UniqueId::gen_uid(), true);
    telemetry._enabled = false;
    telemetry.received(0, 1000);
    EXPECT_EQ(telemetry._windows[0].records, 0);
}

TEST(KafkaReaderTelemetryTest, AggregatesPartitionsIndependently) {
    KafkaReaderTelemetry telemetry(42, UniqueId::gen_uid(), true);
    telemetry._enabled = true;
    telemetry.received(0, 1000);
    telemetry.received(1, 1010);
    telemetry.received(0, 1020);
    EXPECT_EQ(telemetry._windows[0].records, 2);
    EXPECT_EQ(telemetry._windows[0].first_ms, 1000);
    EXPECT_EQ(telemetry._windows[0].last_ms, 1020);
    EXPECT_EQ(telemetry._windows[1].records, 1);
    telemetry.flush();
    EXPECT_EQ(telemetry._windows[0].records, 0);
    EXPECT_EQ(telemetry._windows[1].records, 0);
}

TEST(KafkaReaderTelemetryTest, TimeBoundaryStartsNewWindow) {
    KafkaReaderTelemetry telemetry(42, UniqueId::gen_uid(), false);
    telemetry._enabled = true;
    telemetry.received(0, 1000);
    telemetry.received(0, 1250);
    EXPECT_EQ(telemetry._windows[0].records, 1);
    EXPECT_EQ(telemetry._windows[0].first_ms, 1250);
}

TEST(KafkaReaderTelemetryTest, HashCollisionFlushesOldPartition) {
    KafkaReaderTelemetry telemetry(42, UniqueId::gen_uid(), false);
    telemetry._enabled = true;
    telemetry.received(0, 1000);
    telemetry.received(64, 1010);
    EXPECT_EQ(telemetry._windows[0].partition, 64);
    EXPECT_EQ(telemetry._windows[0].records, 1);
    EXPECT_EQ(telemetry._windows[0].first_ms, 1010);
}

} // namespace doris
