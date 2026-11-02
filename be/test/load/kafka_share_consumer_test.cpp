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

#include <initializer_list>
#include <limits>
#include <thread>
#include <tuple>
#include <vector>

#include "common/env_config.h"
#include "load/routine_load/kafka_reader_telemetry.h"
#include "runtime/thread_context.h"

namespace doris {

namespace {
void prepare_batch(KafkaShareBatch& batch, std::initializer_list<KafkaShareBatch::Record> records,
                   size_t processed) {
    batch.reserve(records.size());
    for (const auto& record : records) {
        batch.add_record(record.first, record.second);
    }
    for (size_t i = 0; i < processed; ++i) {
        batch.mark_processed();
    }
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
        consumer->_batch.add_record(0, 100);
        consumer->_batch.mark_processed();
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
    auto descriptor = consumer.build_source_descriptor(
            42, "cluster-a", {{1, 2}, {1, 2}, {1, 2}, {1, 2}, {1, 2}, {0, 0}});
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
    auto descriptor = consumer.build_source_descriptor(
            42, "cluster-a", {{high_bit, 4}, {high_bit, 4}, {high_bit, 4}, {1, 4}, {1, 4}});
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
    EXPECT_FALSE(consumer.build_source_descriptor(42, "cluster-a", {}).has_value());
    prepare_batch(consumer._batch, {{0, 2}, {0, 4}}, 2);
    EXPECT_FALSE(consumer.build_source_descriptor(42, "", {{1, 2}, {1, 2}}).has_value());
    EXPECT_FALSE(consumer.build_source_descriptor(42, "cluster-a", {{1, 2}, {0, 0}}).has_value());
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
            consumer.build_source_descriptor(ctx.job_id, "cluster-a", {{1, 2}, {1, 2}});
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
    consumer.capture_source_descriptor(&ctx, nullptr);
    EXPECT_FALSE(ctx.kafka_info->source_descriptor.has_value());
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 2, true}, {0, 4, false}}));
    EXPECT_TRUE(consumer.reset().ok());
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
