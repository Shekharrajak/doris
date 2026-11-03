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

import org.apache.doris.common.io.Text;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.OffsetRange;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.Partition;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

public final class KafkaSourceCompletionIndex {
    private static final int SNAPSHOT_VERSION = 1;
    private static final long FIXED_IDENTITY_BYTES = 28;

    private final int maxSourcePartitions;
    private final int maxOffsetRanges;
    private final long maxIdentityBytes;
    private final NavigableMap<SourceKey, RangeSet<Long>> completed = new TreeMap<>();
    private long offsetRangeCount;
    private long identityBytes;

    public KafkaSourceCompletionIndex(int maxSourcePartitions, int maxOffsetRanges, long maxIdentityBytes) {
        Preconditions.checkArgument(maxSourcePartitions >= 0 && maxOffsetRanges >= 0 && maxIdentityBytes >= 0,
                "Kafka source completion limits must be nonnegative");
        this.maxSourcePartitions = maxSourcePartitions;
        this.maxOffsetRanges = maxOffsetRanges;
        this.maxIdentityBytes = maxIdentityBytes;
    }

    public synchronized void add(KafkaSourceDescriptor descriptor) throws CapacityExceededException {
        add(descriptor, true);
    }

    private void add(KafkaSourceDescriptor descriptor, boolean enforceLimits) throws CapacityExceededException {
        AdditionPlan plan = plan(descriptor);
        if (enforceLimits && ((plan.partitionCount > maxSourcePartitions && plan.partitionCount > completed.size())
                || (plan.rangeCount > maxOffsetRanges && plan.rangeCount > offsetRangeCount)
                || (plan.identityBytes > maxIdentityBytes && plan.identityBytes > identityBytes))) {
            throw new CapacityExceededException("Kafka source completion capacity exceeded: partitions="
                    + plan.partitionCount + ", ranges=" + plan.rangeCount + ", identityBytes=" + plan.identityBytes);
        }
        for (Map.Entry<SourceKey, RangeSet<Long>> addition : plan.additions.entrySet()) {
            RangeSet<Long> existing = completed.get(addition.getKey());
            if (existing == null) {
                completed.put(addition.getKey(), addition.getValue());
            } else {
                existing.addAll(addition.getValue());
            }
        }
        offsetRangeCount = plan.rangeCount;
        identityBytes = plan.identityBytes;
    }

    synchronized ReservationCapacity reservationCapacity(KafkaSourceDescriptor descriptor) {
        return plan(descriptor).reservationCapacity;
    }

    synchronized void restore(KafkaSourceDescriptor descriptor) {
        try {
            add(descriptor, false);
        } catch (CapacityExceededException e) {
            throw new IllegalStateException("Recovery cannot reject durable Kafka source completion", e);
        }
    }

    private AdditionPlan plan(KafkaSourceDescriptor descriptor) {
        Map<SourceKey, RangeSet<Long>> additions = new LinkedHashMap<>();
        long nextPartitionCount = completed.size();
        long nextRangeCount = offsetRangeCount;
        long nextIdentityBytes = identityBytes;
        long reservedPartitions = 0;
        long reservedRanges = 0;
        long reservedIdentityBytes = 0;
        long sourceIdentityBytes = SourceKey.identityBytes(descriptor.getClusterId());
        for (Partition partition : descriptor.getPartitions()) {
            SourceKey key = SourceKey.of(descriptor, partition);
            RangeSet<Long> existing = completed.get(key);
            RangeSet<Long> uncovered = TreeRangeSet.create();
            for (OffsetRange range : partition.getRanges()) {
                Range<Long> incoming = toRange(range);
                uncovered.add(incoming);
                if (existing != null) {
                    for (Range<Long> overlap : existing.subRangeSet(incoming).asRanges()) {
                        uncovered.remove(overlap);
                    }
                }
            }
            if (uncovered.isEmpty()) {
                continue;
            }
            if (existing == null) {
                nextPartitionCount++;
                nextIdentityBytes = Math.addExact(nextIdentityBytes, sourceIdentityBytes);
                reservedPartitions++;
                reservedIdentityBytes = Math.addExact(reservedIdentityBytes, sourceIdentityBytes);
            }
            for (Range<Long> range : uncovered.asRanges()) {
                long rangeGrowth = 1;
                long first = range.lowerEndpoint();
                long last = lastOffset(range);
                if (existing != null && first > 0 && existing.contains(first - 1)) {
                    rangeGrowth--;
                }
                if (existing != null && last < Long.MAX_VALUE && existing.contains(last + 1)) {
                    rangeGrowth--;
                }
                nextRangeCount += rangeGrowth;
                // Coalescing savings become available only after the owning commit is durable.
                reservedRanges += Math.max(0, rangeGrowth);
            }
            additions.put(key, uncovered);
        }
        return new AdditionPlan(additions, nextPartitionCount, nextRangeCount, nextIdentityBytes,
                new ReservationCapacity(reservedPartitions, reservedRanges, reservedIdentityBytes));
    }

    synchronized boolean overlaps(KafkaSourceDescriptor query) {
        for (Partition partition : query.getPartitions()) {
            RangeSet<Long> existing = completed.get(SourceKey.of(query, partition));
            if (existing != null) {
                for (OffsetRange range : partition.getRanges()) {
                    if (!existing.subRangeSet(toRange(range)).isEmpty()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // Missing completion evidence does not establish that an offset is new.
    public synchronized Optional<KafkaSourceDescriptor> getCompleted(KafkaSourceDescriptor query) {
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        for (Partition partition : query.getPartitions()) {
            RangeSet<Long> existing = completed.get(SourceKey.of(query, partition));
            if (existing == null) {
                continue;
            }
            RangeSet<Long> intersection = TreeRangeSet.create();
            for (OffsetRange range : partition.getRanges()) {
                intersection.addAll(existing.subRangeSet(toRange(range)));
            }
            if (!intersection.isEmpty()) {
                partitions.add(new Partition(partition.getTopicIdHi(), partition.getTopicIdLo(),
                        partition.getPartition(), toOffsets(intersection)));
            }
        }
        ImmutableList<Partition> result = partitions.build();
        return result.isEmpty() ? Optional.empty()
                : Optional.of(new KafkaSourceDescriptor(query.getNamespaceId(), query.getClusterId(), result));
    }

    public synchronized List<KafkaSourceDescriptor> snapshot() {
        ImmutableList.Builder<KafkaSourceDescriptor> sources = ImmutableList.builder();
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        SourceKey group = null;
        for (Map.Entry<SourceKey, RangeSet<Long>> entry : completed.entrySet()) {
            SourceKey key = entry.getKey();
            if (group != null && (group.namespaceId != key.namespaceId || !group.clusterId.equals(key.clusterId))) {
                sources.add(new KafkaSourceDescriptor(group.namespaceId, group.clusterId, partitions.build()));
                partitions = ImmutableList.builder();
            }
            group = key;
            partitions.add(new Partition(key.topicIdHi, key.topicIdLo, key.partition, toOffsets(entry.getValue())));
        }
        if (group != null) {
            sources.add(new KafkaSourceDescriptor(group.namespaceId, group.clusterId, partitions.build()));
        }
        return sources.build();
    }

    public void write(DataOutput output) throws IOException {
        Text.writeString(output, GsonUtils.GSON.toJson(new Snapshot(snapshot())));
    }

    public static KafkaSourceCompletionIndex read(DataInput input, int maxSourcePartitions, int maxOffsetRanges,
            long maxIdentityBytes) throws IOException {
        KafkaSourceCompletionIndex index = new KafkaSourceCompletionIndex(
                maxSourcePartitions, maxOffsetRanges, maxIdentityBytes);
        try {
            Snapshot snapshot = GsonUtils.GSON.fromJson(Text.readString(input), Snapshot.class);
            if (snapshot == null || snapshot.version == null || snapshot.version != SNAPSHOT_VERSION
                    || snapshot.sources == null) {
                throw new IOException("Invalid Kafka source completion snapshot version or sources");
            }
            for (KafkaSourceDescriptor descriptor : snapshot.sources) {
                index.add(descriptor, false);
            }
            return index;
        } catch (RuntimeException | CapacityExceededException e) {
            throw new IOException("Invalid Kafka source completion snapshot", e);
        }
    }

    public synchronized long getSourcePartitionCount() {
        return completed.size();
    }

    public synchronized long getOffsetRangeCount() {
        return offsetRangeCount;
    }

    public synchronized long getIdentityBytes() {
        return identityBytes;
    }

    private static Range<Long> toRange(OffsetRange range) {
        return range.getLast() == Long.MAX_VALUE ? Range.atLeast(range.getFirst())
                : Range.closedOpen(range.getFirst(), range.getLast() + 1);
    }

    private static long lastOffset(Range<Long> range) {
        return range.hasUpperBound() ? range.upperEndpoint() - 1 : Long.MAX_VALUE;
    }

    private static List<OffsetRange> toOffsets(RangeSet<Long> ranges) {
        ImmutableList.Builder<OffsetRange> offsets = ImmutableList.builder();
        for (Range<Long> range : ranges.asRanges()) {
            offsets.add(new OffsetRange(range.lowerEndpoint(), lastOffset(range)));
        }
        return offsets.build();
    }

    record SourceKey(long namespaceId, String clusterId, long topicIdHi, long topicIdLo, int partition)
            implements Comparable<SourceKey> {
        static SourceKey of(KafkaSourceDescriptor descriptor, Partition partition) {
            return new SourceKey(descriptor.getNamespaceId(), descriptor.getClusterId(),
                    partition.getTopicIdHi(), partition.getTopicIdLo(), partition.getPartition());
        }

        static long identityBytes(String clusterId) {
            return FIXED_IDENTITY_BYTES + clusterId.getBytes(StandardCharsets.UTF_8).length;
        }

        @Override
        public int compareTo(SourceKey other) {
            int result = Long.compare(namespaceId, other.namespaceId);
            if (result == 0) {
                result = clusterId.compareTo(other.clusterId);
            }
            if (result == 0) {
                result = Long.compareUnsigned(topicIdHi, other.topicIdHi);
            }
            if (result == 0) {
                result = Long.compareUnsigned(topicIdLo, other.topicIdLo);
            }
            return result == 0 ? Integer.compare(partition, other.partition) : result;
        }
    }

    private static final class Snapshot {
        @SerializedName("version")
        private final Integer version;
        @SerializedName("sources")
        private final List<KafkaSourceDescriptor> sources;

        private Snapshot(List<KafkaSourceDescriptor> sources) {
            this.version = SNAPSHOT_VERSION;
            this.sources = sources;
        }
    }

    record ReservationCapacity(long sourcePartitions, long offsetRanges, long identityBytes) {
    }

    private record AdditionPlan(Map<SourceKey, RangeSet<Long>> additions, long partitionCount, long rangeCount,
            long identityBytes, ReservationCapacity reservationCapacity) {
    }

    public static final class CapacityExceededException extends Exception {
        public CapacityExceededException(String message) {
            super(message);
        }
    }
}
