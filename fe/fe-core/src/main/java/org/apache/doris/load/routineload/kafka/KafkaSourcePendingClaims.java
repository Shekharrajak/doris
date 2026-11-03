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

package org.apache.doris.load.routineload.kafka;

import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionIndex.CapacityExceededException;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionIndex.SourceKey;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionLedger.SourceConflictException;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.OffsetRange;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.Partition;

import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

// Accessed only under the completion ledger lock.
final class KafkaSourcePendingClaims {
    private final int maxSourcePartitions;
    private final int maxOffsetRanges;
    private final long maxIdentityBytes;
    private final NavigableMap<SourceKey, NavigableMap<Long, Claim>> claims = new TreeMap<>();
    private long offsetRangeCount;
    private long identityBytes;

    KafkaSourcePendingClaims(int maxSourcePartitions, int maxOffsetRanges, long maxIdentityBytes) {
        Preconditions.checkArgument(maxSourcePartitions >= 0 && maxOffsetRanges >= 0 && maxIdentityBytes >= 0,
                "Kafka source claim limits must be nonnegative");
        this.maxSourcePartitions = maxSourcePartitions;
        this.maxOffsetRanges = maxOffsetRanges;
        this.maxIdentityBytes = maxIdentityBytes;
    }

    void reserve(long transactionId, KafkaSourceDescriptor descriptor)
            throws CapacityExceededException, SourceConflictException {
        Map<SourceKey, List<OffsetRange>> additions = new LinkedHashMap<>();
        long nextPartitions = claims.size();
        long nextRanges = offsetRangeCount;
        long nextIdentityBytes = identityBytes;
        long sourceIdentityBytes = SourceKey.identityBytes(descriptor.getClusterId());
        for (Partition partition : descriptor.getPartitions()) {
            SourceKey key = SourceKey.of(descriptor, partition);
            NavigableMap<Long, Claim> existing = claims.get(key);
            if (existing == null) {
                nextPartitions++;
                nextIdentityBytes = Math.addExact(nextIdentityBytes, sourceIdentityBytes);
            }
            for (OffsetRange range : partition.getRanges()) {
                Map.Entry<Long, Claim> overlap = existing == null ? null : existing.floorEntry(range.getLast());
                if (overlap != null && overlap.getValue().last >= range.getFirst()) {
                    throw new SourceConflictException("Kafka source " + key + " range " + range.getFirst()
                            + ".." + range.getLast() + " overlaps pending transaction "
                            + overlap.getValue().transactionId);
                }
                nextRanges = Math.addExact(nextRanges, 1);
            }
            additions.put(key, partition.getRanges());
        }
        if (nextPartitions > maxSourcePartitions || nextRanges > maxOffsetRanges
                || nextIdentityBytes > maxIdentityBytes) {
            throw new CapacityExceededException("Kafka source claim capacity exceeded: partitions="
                    + nextPartitions + "/" + maxSourcePartitions + ", ranges=" + nextRanges + "/"
                    + maxOffsetRanges + ", identityBytes=" + nextIdentityBytes + "/" + maxIdentityBytes);
        }
        for (Map.Entry<SourceKey, List<OffsetRange>> addition : additions.entrySet()) {
            NavigableMap<Long, Claim> ranges = claims.computeIfAbsent(addition.getKey(), key -> new TreeMap<>());
            for (OffsetRange range : addition.getValue()) {
                ranges.put(range.getFirst(), new Claim(range.getLast(), transactionId));
            }
        }
        offsetRangeCount = nextRanges;
        identityBytes = nextIdentityBytes;
    }

    void remove(long transactionId, KafkaSourceDescriptor descriptor) {
        long sourceIdentityBytes = SourceKey.identityBytes(descriptor.getClusterId());
        for (Partition partition : descriptor.getPartitions()) {
            SourceKey key = SourceKey.of(descriptor, partition);
            NavigableMap<Long, Claim> ranges = claims.get(key);
            Preconditions.checkState(ranges != null, "Kafka source claim partition is missing");
            for (OffsetRange range : partition.getRanges()) {
                Claim claim = ranges.get(range.getFirst());
                Preconditions.checkState(claim != null && claim.transactionId == transactionId
                                && claim.last == range.getLast(),
                        "Kafka source claim does not belong to transaction %s", transactionId);
                ranges.remove(range.getFirst());
                offsetRangeCount--;
            }
            if (ranges.isEmpty()) {
                claims.remove(key);
                identityBytes -= sourceIdentityBytes;
            }
        }
    }

    long getSourcePartitionCount() {
        return claims.size();
    }

    long getOffsetRangeCount() {
        return offsetRangeCount;
    }

    long getIdentityBytes() {
        return identityBytes;
    }

    private record Claim(long last, long transactionId) {
    }
}
