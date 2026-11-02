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
#include <cstring>
#include <limits>
#include <tuple>
#include <utility>

#include "common/env_config.h"
#include "common/metrics/doris_metrics.h"
#include "io/fs/stream_load_pipe.h"
#include "load/routine_load/consumer_group_helpers.h"
#include "load/routine_load/kafka_reader_telemetry.h"
#include "runtime/exec_env.h"
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

auto source_partition_key(const TKafkaSourcePartition& partition) {
    return std::tuple(std::bit_cast<uint64_t>(partition.topicId.hi),
                      std::bit_cast<uint64_t>(partition.topicId.lo), partition.partition);
}

Status validate_completed_source(const TKafkaSourceDescriptor& query,
                                 const TKafkaSourceDescriptor& completed) {
    if (!completed.__isset.namespaceId || completed.namespaceId != query.namespaceId ||
        !completed.__isset.clusterId || completed.clusterId != query.clusterId ||
        !completed.__isset.partitions || completed.partitions.empty()) {
        return Status::InternalError("Invalid Kafka source completion descriptor");
    }
    size_t query_index = 0;
    for (size_t i = 0; i < completed.partitions.size(); ++i) {
        const auto& partition = completed.partitions[i];
        if (!partition.__isset.topicId ||
            (partition.topicId.hi == 0 && partition.topicId.lo == 0) ||
            !partition.__isset.partition || partition.partition < 0 || !partition.__isset.ranges ||
            partition.ranges.empty() ||
            (i != 0 && source_partition_key(completed.partitions[i - 1]) >=
                               source_partition_key(partition))) {
            return Status::InternalError("Noncanonical Kafka source completion partition");
        }
        while (query_index < query.partitions.size() &&
               source_partition_key(query.partitions[query_index]) <
                       source_partition_key(partition)) {
            ++query_index;
        }
        if (query_index == query.partitions.size() ||
            source_partition_key(query.partitions[query_index]) !=
                    source_partition_key(partition)) {
            return Status::InternalError("Kafka source completion is outside the query");
        }
        const auto& query_ranges = query.partitions[query_index].ranges;
        size_t range_index = 0;
        for (size_t j = 0; j < partition.ranges.size(); ++j) {
            const auto& range = partition.ranges[j];
            if (!range.__isset.first || !range.__isset.last || range.first < 0 ||
                range.last < range.first ||
                (j != 0 && (partition.ranges[j - 1].last == std::numeric_limits<int64_t>::max() ||
                            range.first <= partition.ranges[j - 1].last + 1))) {
                return Status::InternalError("Noncanonical Kafka source completion range");
            }
            while (range_index < query_ranges.size() &&
                   query_ranges[range_index].last < range.first) {
                ++range_index;
            }
            if (range_index == query_ranges.size() ||
                query_ranges[range_index].first > range.first ||
                query_ranges[range_index].last < range.last) {
                return Status::InternalError("Kafka source completion is outside the query");
            }
        }
    }
    return Status::OK();
}

bool source_completed(const TKafkaSourceDescriptor& completed,
                      const KafkaShareBatch::Record& record) {
    auto key = std::tuple(record.topic_id_hi, record.topic_id_lo, record.partition);
    auto partition = std::lower_bound(completed.partitions.begin(), completed.partitions.end(), key,
                                      [](const auto& value, const auto& target) {
                                          return source_partition_key(value) < target;
                                      });
    if (partition == completed.partitions.end() || source_partition_key(*partition) != key) {
        return false;
    }
    auto range =
            std::lower_bound(partition->ranges.begin(), partition->ranges.end(), record.offset,
                             [](const auto& value, int64_t offset) { return value.last < offset; });
    return range != partition->ranges.end() && range->first <= record.offset;
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
    _records.push_back(
            {.partition = partition, .offset = offset, .representative = _records.size()});
}

void KafkaShareBatch::set_topic_id(size_t index, uint64_t hi, uint64_t lo) {
    _records[index].topic_id_hi = hi;
    _records[index].topic_id_lo = lo;
}

void KafkaShareBatch::mark_done(size_t index) {
    DCHECK(_records[index].disposition == Disposition::UNTOUCHED ||
           _records[index].disposition == Disposition::DUPLICATE);
    _records[index].disposition = Disposition::DURABLE_DONE;
}

void KafkaShareBatch::mark_appended(size_t index) {
    DCHECK(_records[index].disposition == Disposition::UNTOUCHED);
    _records[index].disposition = Disposition::APPENDED;
    ++_processed;
}

void KafkaShareBatch::mark_duplicate(size_t index, size_t representative) {
    DCHECK_LT(representative, index);
    DCHECK(_records[representative].disposition != Disposition::DUPLICATE);
    DCHECK(_records[index].disposition == Disposition::UNTOUCHED);
    _records[index].disposition = Disposition::DUPLICATE;
    _records[index].representative = representative;
}

bool KafkaShareBatch::should_accept(size_t index, bool committed) const {
    const auto& record = _records[index];
    auto disposition = record.disposition == Disposition::DUPLICATE
                               ? _records[record.representative].disposition
                               : record.disposition;
    return disposition == Disposition::DURABLE_DONE ||
           (committed && disposition == Disposition::APPENDED);
}

bool KafkaShareBatch::all_records_completed() const {
    if (empty()) {
        return false;
    }
    for (size_t i = 0; i < size(); ++i) {
        if (!should_accept(i, false)) {
            return false;
        }
    }
    return true;
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
#ifndef HAVE_RDKAFKA_MESSAGE_TOPIC_ID
    if (config::enable_kafka_share_completion_filtering) {
        return Status::InternalError(
                "Kafka share completion filtering requires the topic identity API");
    }
#endif
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
    const bool filtering = config::enable_kafka_share_completion_filtering;
    ctx->kafka_info->source_descriptor.reset();
#ifndef HAVE_RDKAFKA_MESSAGE_TOPIC_ID
    if (filtering) {
        return Status::InternalError(
                "Kafka share completion filtering requires the topic identity API");
    }
#endif
    rd_kafka_messages_t* batch = nullptr;
    Defer destroy_batch {[&] { rd_kafka_messages_destroy(batch); }};
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
    const int64_t poll_ns = watch.elapsed_time();
    const size_t count = batch == nullptr ? 0 : rd_kafka_messages_count(batch);
    DorisVector<rd_kafka_message_t*> messages;
    Defer free_messages {[&] {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_batch.mem_tracker());
        decltype(messages)().swap(messages);
    }};
    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_batch.mem_tracker());
        messages.reserve(count);
    }
    for (size_t i = 0; i < count; ++i) {
        messages.push_back(rd_kafka_messages_get(batch, i));
    }
    return consume_messages(ctx, messages, filtering, poll_ns);
}

Status KafkaShareConsumer::consume_messages(std::shared_ptr<StreamLoadContext> ctx,
                                            std::span<rd_kafka_message_t* const> messages,
                                            bool filtering, int64_t poll_ns) {
    DCHECK(_batch.empty());
    ctx->kafka_info->source_descriptor.reset();
    KafkaReaderTelemetry telemetry(ctx->job_id, _id, true);
    telemetry.timing("poll", poll_ns);
    share_received_records << messages.size();
    _batch.reserve(messages.size());
    for (auto* message : messages) {
        _batch.add_record(message->partition, message->offset);
        if (telemetry.enabled() && message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
            telemetry.received(message->partition, UnixMillis());
        }
    }
    telemetry.flush();
    std::string cluster_id;
    if (filtering && !messages.empty()) {
        for (auto* message : messages) {
            if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                return Status::InternalError("Kafka share record error: {}",
                                             rd_kafka_err2str(message->err));
            }
        }
        RETURN_IF_ERROR(capture_source_identity(messages, false, &cluster_id));
        RETURN_IF_ERROR(classify_records(messages));
        auto query = build_source_descriptor(ctx->job_id, cluster_id, false);
        if (!query.has_value()) {
            return Status::InternalError("Missing Kafka share source identity");
        }
        std::optional<TKafkaSourceDescriptor> completed;
        auto lookup_status = ctx->exec_env()->stream_load_executor()->get_kafka_source_completions(
                ctx.get(), *query, &completed);
        if (!lookup_status.ok()) {
            return Status::InternalError("Kafka source completion lookup failed: {}",
                                         lookup_status);
        }
        if (completed.has_value()) {
            RETURN_IF_ERROR(validate_completed_source(*query, *completed));
            for (size_t i = 0; i < messages.size(); ++i) {
                if (source_completed(*completed, _batch.record(i))) {
                    _batch.mark_done(i);
                }
            }
        }
        for (size_t i = 0; i < messages.size(); ++i) {
            if (messages[i]->payload == nullptr && !_batch.should_accept(i, false)) {
                return Status::InternalError("Uncompleted Kafka share record has a null payload");
            }
        }
    }
    auto pipe = std::static_pointer_cast<io::StreamLoadPipe>(ctx->body_sink);
    auto append = FormatAppender::get_append_function<io::StreamLoadPipe>(ctx->format);
    int64_t bytes = 0;
    const int64_t append_start = MonotonicNanos();
    for (size_t i = 0; i < messages.size(); ++i) {
        if (filtering && _batch.record(i).disposition != KafkaShareBatch::Disposition::UNTOUCHED) {
            continue;
        }
        if (std::cmp_greater_equal(_batch.processed(), ctx->max_batch_rows) ||
            bytes >= ctx->max_batch_size) {
            break;
        }
        auto* message = messages[i];
        if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            return Status::InternalError("Kafka share record error: {}",
                                         rd_kafka_err2str(message->err));
        }
        if (message->payload != nullptr) {
            RETURN_IF_ERROR((pipe.get()->*append)(static_cast<const char*>(message->payload),
                                                  message->len));
        }
        _batch.mark_appended(i);
        bytes += message->len;
    }
    telemetry.timing("append", MonotonicNanos() - append_start);
    ctx->receive_bytes = bytes;
    DorisMetrics::instance()->routine_load_consume_rows->increment(_batch.processed());
    DorisMetrics::instance()->routine_load_consume_bytes->increment(bytes);
    if (!filtering && _batch.processed() != 0) {
        if (!capture_source_identity(messages, true, &cluster_id).ok()) {
            cluster_id.clear();
        }
    }
    ctx->kafka_info->source_descriptor = build_source_descriptor(ctx->job_id, cluster_id, true);
    return pipe->finish();
}

Status KafkaShareConsumer::capture_source_identity(
        [[maybe_unused]] std::span<rd_kafka_message_t* const> messages,
        [[maybe_unused]] bool appended_only, [[maybe_unused]] std::string* cluster_id) {
#ifdef HAVE_RDKAFKA_MESSAGE_TOPIC_ID
    if (_client == nullptr) {
        return Status::InternalError("Missing Kafka share client identity");
    }
    char* native_cluster_id = rd_kafka_clusterid(_client, 0);
    if (native_cluster_id == nullptr) {
        return Status::InternalError("Missing Kafka share cluster identity");
    }
    Defer free_cluster_id {[&] { rd_kafka_mem_free(_client, native_cluster_id); }};
    *cluster_id = native_cluster_id;
    if (cluster_id->empty()) {
        return Status::InternalError("Missing Kafka share cluster identity");
    }
    for (size_t i = 0; i < messages.size(); ++i) {
        if (appended_only &&
            _batch.record(i).disposition != KafkaShareBatch::Disposition::APPENDED) {
            continue;
        }
        const auto* topic_id = rd_kafka_message_topic_id(messages[i]);
        if (topic_id == nullptr) {
            return Status::InternalError("Missing Kafka share delivered topic identity");
        }
        auto hi = static_cast<uint64_t>(rd_kafka_Uuid_most_significant_bits(topic_id));
        auto lo = static_cast<uint64_t>(rd_kafka_Uuid_least_significant_bits(topic_id));
        if (hi == 0 && lo == 0) {
            return Status::InternalError("Missing Kafka share delivered topic identity");
        }
        _batch.set_topic_id(i, hi, lo);
    }
    return Status::OK();
#else
    return Status::InternalError(
            "Kafka share completion filtering requires the topic identity API");
#endif
}

Status KafkaShareConsumer::classify_records(std::span<rd_kafka_message_t* const> messages) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_batch.mem_tracker());
    DorisMap<std::tuple<uint64_t, uint64_t, int32_t, int64_t>, size_t> representatives;
    DorisMap<int32_t, TopicId> partition_topics;
    for (size_t i = 0; i < messages.size(); ++i) {
        const auto& record = _batch.record(i);
        const TopicId topic_id(record.topic_id_hi, record.topic_id_lo);
        if ((topic_id.first == 0 && topic_id.second == 0) || record.partition < 0 ||
            record.offset < 0) {
            return Status::InternalError("Missing Kafka share source identity");
        }
        auto [topic, new_partition] = partition_topics.emplace(record.partition, topic_id);
        if (!new_partition && topic->second != topic_id) {
            return Status::InternalError(
                    "Multiple Kafka topic identities for one acquired partition");
        }
        auto [representative, inserted] = representatives.emplace(
                std::tuple(record.topic_id_hi, record.topic_id_lo, record.partition, record.offset),
                i);
        if (inserted) {
            continue;
        }
        auto* original = messages[representative->second];
        auto* duplicate = messages[i];
        if ((original->payload == nullptr) != (duplicate->payload == nullptr) ||
            original->len != duplicate->len ||
            (original->payload != nullptr && original->len != 0 &&
             std::memcmp(original->payload, duplicate->payload, original->len) != 0)) {
            return Status::InternalError(
                    "Inconsistent payloads for one Kafka share source identity");
        }
        _batch.mark_duplicate(i, representative->second);
    }
    return Status::OK();
}

std::optional<TKafkaSourceDescriptor> KafkaShareConsumer::build_source_descriptor(
        int64_t namespace_id, const std::string& cluster_id, bool appended_only) const {
    if ((appended_only ? _batch.processed() == 0 : _batch.empty()) || cluster_id.empty()) {
        return std::nullopt;
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_batch.mem_tracker());
    DorisVector<std::tuple<uint64_t, uint64_t, int32_t, int64_t>> records;
    records.reserve(appended_only ? _batch.processed() : _batch.size());
    for (size_t i = 0; i < _batch.size(); ++i) {
        const auto& record = _batch.record(i);
        if (appended_only && record.disposition != KafkaShareBatch::Disposition::APPENDED) {
            continue;
        }
        if ((record.topic_id_hi == 0 && record.topic_id_lo == 0) || record.partition < 0 ||
            record.offset < 0) {
            return std::nullopt;
        }
        records.emplace_back(record.topic_id_hi, record.topic_id_lo, record.partition,
                             record.offset);
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
        auto st = acknowledge_record(record.partition, record.offset,
                                     _batch.should_accept(i, committed));
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
