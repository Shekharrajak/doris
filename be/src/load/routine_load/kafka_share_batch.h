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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "core/custom_allocator.h"

namespace doris {

class MemTrackerLimiter;

class KafkaShareBatch {
public:
    enum class Disposition { UNTOUCHED, DURABLE_DONE, APPENDED, DUPLICATE };

    struct Record {
        int32_t partition;
        int64_t offset;
        uint64_t topic_id_hi = 0;
        uint64_t topic_id_lo = 0;
        Disposition disposition = Disposition::UNTOUCHED;
        size_t representative = 0;
    };

    KafkaShareBatch();
    ~KafkaShareBatch();

    KafkaShareBatch(const KafkaShareBatch&) = delete;
    KafkaShareBatch& operator=(const KafkaShareBatch&) = delete;
    KafkaShareBatch(KafkaShareBatch&&) = delete;
    KafkaShareBatch& operator=(KafkaShareBatch&&) = delete;

    void reserve(size_t count);
    void add_record(int32_t partition, int64_t offset);
    void set_topic_id(size_t index, uint64_t hi, uint64_t lo);
    void mark_done(size_t index);
    void mark_appended(size_t index);
    void mark_duplicate(size_t index, size_t representative);
    bool should_accept(size_t index, bool committed) const;
    bool all_records_completed() const;
    void clear();

    bool empty() const { return _records.empty(); }
    size_t size() const { return _records.size(); }
    size_t capacity() const { return _records.capacity(); }
    size_t processed() const { return _processed; }
    const Record& record(size_t index) const { return _records[index]; }
    const std::shared_ptr<MemTrackerLimiter>& mem_tracker() const { return _mem_tracker; }

private:
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    DorisVector<Record> _records;
    size_t _processed = 0;
};

} // namespace doris
