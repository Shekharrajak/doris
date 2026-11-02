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

#include <bvar/bvar.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <tuple>
#include <utility>

#include "common/env_config.h"
#include "common/metrics/doris_metrics.h"
#include "io/fs/stream_load_pipe.h"
#include "load/routine_load/consumer_group_helpers.h"
#include "load/routine_load/kafka_reader_telemetry.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"

namespace doris {

namespace {
bvar::Adder<int64_t> share_consumers("routine_load_share_consumers");
bvar::Adder<int64_t> share_polling_consumers("routine_load_share_polling_consumers");
bvar::Adder<int64_t> share_received_records("routine_load_share_received_records");
bvar::Adder<int64_t> share_committed_records("routine_load_share_committed_records");
bvar::Adder<int64_t> share_ack_failures("routine_load_share_ack_failures");

Status share_status(rd_kafka_error_t* error) {
    if (error == nullptr) {
        return Status::OK();
    }
    auto status = Status::InternalError("Kafka share consumer: {}", rd_kafka_error_string(error));
    rd_kafka_error_destroy(error);
    return status;
}
} // namespace

KafkaShareBatch::KafkaShareBatch()
        : _mem_tracker(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "KafkaShareConsumer")) {}

KafkaShareBatch::~KafkaShareBatch() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    decltype(_records)().swap(_records);
}

void KafkaShareBatch::reserve(size_t count) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    _records.reserve(count);
}

void KafkaShareBatch::add_record(int32_t partition, int64_t offset) {
    // reserve() binds capacity allocations to the batch tracker before appending.
    DCHECK_LT(_records.size(), _records.capacity());
    _records.emplace_back(partition, offset);
}

void KafkaShareBatch::mark_processed() {
    DCHECK_LT(_processed, _records.size());
    ++_processed;
}

void KafkaShareBatch::clear() {
    _records.clear();
    _processed = 0;
}

KafkaShareConsumer::KafkaShareConsumer() = default;

KafkaShareConsumer::~KafkaShareConsumer() {
    if (_consumer != nullptr) {
        if (!_batch.empty()) {
            auto status = acknowledge(false);
            if (!status.ok()) {
                LOG(WARNING) << "Failed to release share batch: " << status;
            }
        }
        auto status = share_status(rd_kafka_share_consumer_close(_consumer));
        if (!status.ok()) {
            LOG(WARNING) << "Failed to close share consumer: " << status;
        }
        rd_kafka_share_destroy(_consumer);
        share_consumers << -1;
    }
}

Status KafkaShareConsumer::init(std::shared_ptr<StreamLoadContext> ctx) {
    _brokers = ctx->kafka_info->brokers;
    _topic = ctx->kafka_info->topic;
    _properties = ctx->kafka_info->properties;
    auto* conf = rd_kafka_conf_new();
    Defer destroy_conf {[&] {
        if (conf != nullptr) {
            rd_kafka_conf_destroy(conf);
        }
    }};
    auto set = [&](const std::string& key, const std::string& value) -> Status {
        char error[512];
        if (rd_kafka_conf_set(conf, key.c_str(), value.c_str(), error, sizeof(error)) !=
            RD_KAFKA_CONF_OK) {
            return Status::InvalidArgument("PAUSE: Invalid Kafka share property: {}", key);
        }
        return Status::OK();
    };
    for (const auto& [key, value] : _properties) {
        RETURN_IF_ERROR(set(key, value));
    }
    RETURN_IF_ERROR(set("bootstrap.servers", _brokers));
    RETURN_IF_ERROR(set("share.acknowledgement.mode", "explicit"));
    RETURN_IF_ERROR(set("socket.timeout.ms", "10000"));
#ifdef HAVE_RDKAFKA_MESSAGE_TOPIC_ID
    auto interceptor_error = rd_kafka_conf_interceptor_add_on_new(
            conf, "doris-share-source-identity",
            [](rd_kafka_t* client, const rd_kafka_conf_t*, void* opaque, char*, size_t) {
                *static_cast<rd_kafka_t**>(opaque) = client;
                return RD_KAFKA_RESP_ERR_NO_ERROR;
            },
            &_client);
    if (interceptor_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return Status::InternalError("Kafka share source identity interceptor: {}",
                                     rd_kafka_err2str(interceptor_error));
    }
#endif
    char error[512];
    _consumer = rd_kafka_share_consumer_new(conf, error, sizeof(error));
    if (_consumer == nullptr) {
        _client = nullptr;
        return Status::InternalError("PAUSE: Failed to create Kafka share consumer: {}", error);
    }
    share_consumers << 1;
    conf = nullptr;
    auto* topics = rd_kafka_topic_partition_list_new(1);
    Defer destroy_topics {[&] { rd_kafka_topic_partition_list_destroy(topics); }};
    rd_kafka_topic_partition_list_add(topics, _topic.c_str(), RD_KAFKA_PARTITION_UA);
    auto subscribe_error = rd_kafka_share_subscribe(_consumer, topics);
    if (subscribe_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return Status::InternalError("Kafka share subscribe: {}",
                                     rd_kafka_err2str(subscribe_error));
    }
    _init = true;
    _last_visit_time = time(nullptr);
    return Status::OK();
}

Status KafkaShareConsumer::consume(std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(_batch.empty());
    ctx->kafka_info->source_descriptor.reset();
    KafkaReaderTelemetry telemetry(ctx->job_id, _id, true);
    rd_kafka_messages_t* batch = nullptr;
    Defer destroy_batch {[&] { rd_kafka_messages_destroy(batch); }};
    auto pipe = std::static_pointer_cast<io::StreamLoadPipe>(ctx->body_sink);
    MonotonicStopWatch watch;
    watch.start();
    int64_t remaining_ms = ctx->max_interval_s * 1000;
    share_polling_consumers << 1;
    Defer stop_polling {[&] { share_polling_consumers << -1; }};
    while (batch == nullptr && remaining_ms > 0) {
        RETURN_IF_ERROR(share_status(rd_kafka_share_poll(
                _consumer, static_cast<int>(std::min<int64_t>(remaining_ms, 1000)), &batch)));
        remaining_ms = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000000;
    }
    telemetry.timing("poll", watch.elapsed_time());
    auto append = FormatAppender::get_append_function<io::StreamLoadPipe>(ctx->format);
    int64_t bytes = 0;
    size_t count = batch == nullptr ? 0 : rd_kafka_messages_count(batch);
    share_received_records << count;
    _batch.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        auto* message = rd_kafka_messages_get(batch, i);
        _batch.add_record(message->partition, message->offset);
        if (telemetry.enabled() && message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
            telemetry.received(message->partition, UnixMillis());
        }
    }
    telemetry.flush();
    const int64_t append_start = MonotonicNanos();
    for (size_t i = 0; i < count; ++i) {
        if (std::cmp_greater_equal(_batch.processed(), ctx->max_batch_rows) ||
            bytes >= ctx->max_batch_size) {
            break;
        }
        auto* message = rd_kafka_messages_get(batch, i);
        if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            return Status::InternalError("Kafka share record error: {}",
                                         rd_kafka_err2str(message->err));
        }
        if (message->payload != nullptr) {
            RETURN_IF_ERROR((pipe.get()->*append)(static_cast<const char*>(message->payload),
                                                  message->len));
        }
        _batch.mark_processed();
        bytes += message->len;
    }
    telemetry.timing("append", MonotonicNanos() - append_start);
    ctx->receive_bytes = bytes;
    DorisMetrics::instance()->routine_load_consume_rows->increment(_batch.processed());
    DorisMetrics::instance()->routine_load_consume_bytes->increment(bytes);
    capture_source_descriptor(ctx.get(), batch);
    return pipe->finish();
}

void KafkaShareConsumer::capture_source_descriptor(StreamLoadContext* ctx,
                                                   [[maybe_unused]] rd_kafka_messages_t* messages) {
    ctx->kafka_info->source_descriptor.reset();
#ifdef HAVE_RDKAFKA_MESSAGE_TOPIC_ID
    if (_batch.processed() == 0) {
        return;
    }
    char* cluster_id = rd_kafka_clusterid(_client, 0);
    if (cluster_id == nullptr) {
        return;
    }
    Defer free_cluster_id {[&] { rd_kafka_mem_free(_client, cluster_id); }};
    std::vector<TopicId> topic_ids;
    topic_ids.reserve(_batch.processed());
    for (size_t i = 0; i < _batch.processed(); ++i) {
        auto* message = rd_kafka_messages_get(messages, i);
        const auto* topic_id = rd_kafka_message_topic_id(message);
        if (topic_id == nullptr) {
            topic_ids.emplace_back(0, 0);
        } else {
            topic_ids.emplace_back(
                    static_cast<uint64_t>(rd_kafka_Uuid_most_significant_bits(topic_id)),
                    static_cast<uint64_t>(rd_kafka_Uuid_least_significant_bits(topic_id)));
        }
    }
    ctx->kafka_info->source_descriptor =
            build_source_descriptor(ctx->job_id, cluster_id, topic_ids);
#endif
}

std::optional<TKafkaSourceDescriptor> KafkaShareConsumer::build_source_descriptor(
        int64_t namespace_id, const std::string& cluster_id,
        const std::vector<TopicId>& topic_ids) const {
    if (_batch.processed() == 0 || cluster_id.empty()) {
        return std::nullopt;
    }
    DCHECK_GE(topic_ids.size(), _batch.processed());
    std::vector<std::tuple<uint64_t, uint64_t, int32_t, int64_t>> records;
    records.reserve(_batch.processed());
    for (size_t i = 0; i < _batch.processed(); ++i) {
        const auto& [hi, lo] = topic_ids[i];
        if (hi == 0 && lo == 0) {
            return std::nullopt;
        }
        const auto& [partition, offset] = _batch.record(i);
        records.emplace_back(hi, lo, partition, offset);
    }
    std::sort(records.begin(), records.end());
    std::vector<TKafkaSourcePartition> partitions;
    for (const auto& [hi, lo, partition, offset] : records) {
        TUniqueId topic_id;
        topic_id.__set_hi(std::bit_cast<int64_t>(hi));
        topic_id.__set_lo(std::bit_cast<int64_t>(lo));
        if (partitions.empty() || partitions.back().topicId != topic_id ||
            partitions.back().partition != partition) {
            TKafkaSourcePartition source_partition;
            source_partition.__set_topicId(topic_id);
            source_partition.__set_partition(partition);
            source_partition.__set_ranges({});
            partitions.push_back(std::move(source_partition));
        }
        auto& ranges = partitions.back().ranges;
        if (!ranges.empty() && (offset <= ranges.back().last ||
                                (ranges.back().last != std::numeric_limits<int64_t>::max() &&
                                 offset == ranges.back().last + 1))) {
            ranges.back().__set_last(std::max(ranges.back().last, offset));
        } else {
            TKafkaSourceOffsetRange range;
            range.__set_first(offset);
            range.__set_last(offset);
            ranges.push_back(std::move(range));
        }
    }
    TKafkaSourceDescriptor descriptor;
    descriptor.__set_namespaceId(namespace_id);
    descriptor.__set_clusterId(cluster_id);
    descriptor.__set_partitions(partitions);
    return descriptor;
}

Status KafkaShareConsumer::acknowledge(bool committed) {
    if (_batch.empty()) {
        return Status::OK();
    }
    _reusable = false;
    Defer clear_batch {[&] { _batch.clear(); }};
    Status status;
    bool staged = false;
    for (size_t i = 0; i < _batch.size(); ++i) {
        const auto& record = _batch.record(i);
        auto st = acknowledge_record(record.first, record.second,
                                     committed && i < _batch.processed());
        if (st.ok()) {
            staged = true;
        } else if (status.ok()) {
            status = st;
        }
    }
    // An expired acquisition must not strand acknowledgements for the remaining records.
    if (staged) {
        auto st = commit_acknowledgements();
        if (status.ok()) {
            status = st;
        }
    }
    _reusable = status.ok();
    return status;
}

Status KafkaShareConsumer::acknowledge_record(int32_t partition, int64_t offset, bool accept) {
    auto type = accept ? RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT
                       : RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE;
    auto error =
            rd_kafka_share_acknowledge_offset(_consumer, _topic.c_str(), partition, offset, type);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return Status::InternalError("Kafka share acknowledgement: {}", rd_kafka_err2str(error));
    }
    return Status::OK();
}

Status KafkaShareConsumer::commit_acknowledgements() {
    rd_kafka_topic_partition_list_t* results = nullptr;
    Defer destroy_results {[&] {
        if (results != nullptr) {
            rd_kafka_topic_partition_list_destroy(results);
        }
    }};
    RETURN_IF_ERROR(share_status(rd_kafka_share_commit_sync(_consumer, 10000, &results)));
    if (results != nullptr) {
        for (int i = 0; i < results->cnt; ++i) {
            const auto& result = results->elems[i];
            if (result.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                return Status::InternalError("Kafka share acknowledgement partition {}: {}",
                                             result.partition, rd_kafka_err2str(result.err));
            }
        }
    }
    return Status::OK();
}

Status KafkaShareConsumer::acknowledge_committed() {
    share_committed_records << _batch.processed();
    auto status = acknowledge(true);
    if (!status.ok()) {
        share_ack_failures << 1;
    }
    return status;
}

Status KafkaShareConsumer::cancel(std::shared_ptr<StreamLoadContext> ctx) {
    return acknowledge(false);
}

Status KafkaShareConsumer::reset() {
    if (!_batch.empty() || !_reusable) {
        return Status::InternalError("Cannot reuse an unresolved Kafka share consumer");
    }
    _last_visit_time = time(nullptr);
    return Status::OK();
}

bool KafkaShareConsumer::match(std::shared_ptr<StreamLoadContext> ctx) {
    return ctx->load_src_type == TLoadSourceType::KAFKA && ctx->kafka_info->use_share_consumer &&
           _reusable && _batch.empty() && _brokers == ctx->kafka_info->brokers &&
           _topic == ctx->kafka_info->topic && _properties == ctx->kafka_info->properties;
}

} // namespace doris
