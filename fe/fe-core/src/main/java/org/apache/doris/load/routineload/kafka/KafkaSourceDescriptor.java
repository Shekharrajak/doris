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

import org.apache.doris.cloud.proto.Cloud.KafkaSourceDescriptorPB;
import org.apache.doris.cloud.proto.Cloud.KafkaSourceOffsetRangePB;
import org.apache.doris.cloud.proto.Cloud.KafkaSourcePartitionPB;
import org.apache.doris.cloud.proto.Cloud.UniqueIdPB;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.thrift.TKafkaSourceDescriptor;
import org.apache.doris.thrift.TKafkaSourceOffsetRange;
import org.apache.doris.thrift.TKafkaSourcePartition;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public final class KafkaSourceDescriptor implements GsonPostProcessable {
    // Boxed scalars distinguish absent persisted fields from valid zero values.
    @SerializedName("namespaceId")
    private final Long namespaceId;
    @SerializedName("clusterId")
    private final String clusterId;
    @SerializedName("partitions")
    private final ImmutableList<Partition> partitions;

    public KafkaSourceDescriptor(long namespaceId, String clusterId, List<Partition> partitions) {
        this.namespaceId = namespaceId;
        this.clusterId = clusterId;
        this.partitions = ImmutableList.copyOf(partitions);
        validate();
    }

    public static KafkaSourceDescriptor fromThrift(TKafkaSourceDescriptor descriptor) {
        Preconditions.checkArgument(descriptor.isSetNamespaceId() && descriptor.isSetClusterId()
                && descriptor.isSetPartitions(), "Incomplete Kafka source descriptor");
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        for (TKafkaSourcePartition partition : descriptor.getPartitions()) {
            Preconditions.checkArgument(partition != null && partition.isSetTopicId() && partition.isSetPartition()
                    && partition.isSetRanges(), "Incomplete Kafka source partition");
            Preconditions.checkArgument(partition.getTopicId().isSetHi() && partition.getTopicId().isSetLo(),
                    "Incomplete Kafka source topic UUID");
            ImmutableList.Builder<OffsetRange> ranges = ImmutableList.builder();
            for (TKafkaSourceOffsetRange range : partition.getRanges()) {
                Preconditions.checkArgument(range != null && range.isSetFirst() && range.isSetLast(),
                        "Incomplete Kafka source offset range");
                ranges.add(new OffsetRange(range.getFirst(), range.getLast()));
            }
            partitions.add(new Partition(partition.getTopicId().getHi(), partition.getTopicId().getLo(),
                    partition.getPartition(), ranges.build()));
        }
        return new KafkaSourceDescriptor(descriptor.getNamespaceId(), descriptor.getClusterId(), partitions.build());
    }

    public static KafkaSourceDescriptor fromPb(KafkaSourceDescriptorPB descriptor) {
        Preconditions.checkArgument(descriptor.hasNamespaceId() && descriptor.hasClusterId(),
                "Incomplete Kafka source descriptor");
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        for (KafkaSourcePartitionPB partition : descriptor.getPartitionsList()) {
            Preconditions.checkArgument(partition.hasTopicId() && partition.hasPartition(),
                    "Incomplete Kafka source partition");
            Preconditions.checkArgument(partition.getTopicId().hasHi() && partition.getTopicId().hasLo(),
                    "Incomplete Kafka source topic UUID");
            ImmutableList.Builder<OffsetRange> ranges = ImmutableList.builder();
            for (KafkaSourceOffsetRangePB range : partition.getRangesList()) {
                Preconditions.checkArgument(range.hasFirst() && range.hasLast(),
                        "Incomplete Kafka source offset range");
                ranges.add(new OffsetRange(range.getFirst(), range.getLast()));
            }
            partitions.add(new Partition(partition.getTopicId().getHi(), partition.getTopicId().getLo(),
                    partition.getPartition(), ranges.build()));
        }
        return new KafkaSourceDescriptor(descriptor.getNamespaceId(), descriptor.getClusterId(), partitions.build());
    }

    public KafkaSourceDescriptorPB toPb() {
        KafkaSourceDescriptorPB.Builder descriptor = KafkaSourceDescriptorPB.newBuilder()
                .setNamespaceId(namespaceId).setClusterId(clusterId);
        for (Partition partition : partitions) {
            KafkaSourcePartitionPB.Builder sourcePartition = KafkaSourcePartitionPB.newBuilder()
                    .setTopicId(UniqueIdPB.newBuilder().setHi(partition.topicIdHi).setLo(partition.topicIdLo))
                    .setPartition(partition.partition);
            for (OffsetRange range : partition.ranges) {
                sourcePartition.addRanges(KafkaSourceOffsetRangePB.newBuilder()
                        .setFirst(range.first).setLast(range.last));
            }
            descriptor.addPartitions(sourcePartition);
        }
        return descriptor.build();
    }

    public TKafkaSourceDescriptor toThrift() {
        TKafkaSourceDescriptor descriptor = new TKafkaSourceDescriptor()
                .setNamespaceId(namespaceId).setClusterId(clusterId);
        for (Partition partition : partitions) {
            TKafkaSourcePartition sourcePartition = new TKafkaSourcePartition()
                    .setTopicId(new TUniqueId(partition.topicIdHi, partition.topicIdLo))
                    .setPartition(partition.partition);
            for (OffsetRange range : partition.ranges) {
                sourcePartition.addToRanges(new TKafkaSourceOffsetRange()
                        .setFirst(range.first).setLast(range.last));
            }
            descriptor.addToPartitions(sourcePartition);
        }
        return descriptor;
    }

    public long getNamespaceId() {
        return namespaceId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public void validateForJob(long jobId) {
        Preconditions.checkArgument(namespaceId == jobId, "Kafka source namespace does not match routine load job");
    }

    private void validate() {
        Preconditions.checkArgument(namespaceId != null && namespaceId > 0, "Kafka source namespace must be positive");
        Preconditions.checkArgument(clusterId != null && !clusterId.isBlank(), "Kafka source cluster ID is empty");
        Preconditions.checkArgument(partitions != null && !partitions.isEmpty(), "Kafka source partitions are empty");
        Partition previous = null;
        for (Partition partition : partitions) {
            Preconditions.checkArgument(partition != null, "Kafka source partition is null");
            partition.validate();
            Preconditions.checkArgument(previous == null || previous.compareTo(partition) < 0,
                    "Kafka source partitions must be sorted and unique");
            previous = partition;
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        try {
            validate();
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid Kafka source descriptor", e);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof KafkaSourceDescriptor)) {
            return false;
        }
        KafkaSourceDescriptor descriptor = (KafkaSourceDescriptor) other;
        return Objects.equals(namespaceId, descriptor.namespaceId) && Objects.equals(clusterId, descriptor.clusterId)
                && Objects.equals(partitions, descriptor.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespaceId, clusterId, partitions);
    }

    public static final class Partition implements GsonPostProcessable {
        @SerializedName("topicIdHi")
        private final Long topicIdHi;
        @SerializedName("topicIdLo")
        private final Long topicIdLo;
        @SerializedName("partition")
        private final Integer partition;
        @SerializedName("ranges")
        private final ImmutableList<OffsetRange> ranges;

        public Partition(long topicIdHi, long topicIdLo, int partition, List<OffsetRange> ranges) {
            this.topicIdHi = topicIdHi;
            this.topicIdLo = topicIdLo;
            this.partition = partition;
            this.ranges = ImmutableList.copyOf(ranges);
            validate();
        }

        public long getTopicIdHi() {
            return topicIdHi;
        }

        public long getTopicIdLo() {
            return topicIdLo;
        }

        public int getPartition() {
            return partition;
        }

        public List<OffsetRange> getRanges() {
            return ranges;
        }

        private int compareTo(Partition other) {
            int result = Long.compareUnsigned(topicIdHi, other.topicIdHi);
            if (result == 0) {
                result = Long.compareUnsigned(topicIdLo, other.topicIdLo);
            }
            return result == 0 ? Integer.compare(partition, other.partition) : result;
        }

        private void validate() {
            Preconditions.checkArgument(topicIdHi != null && topicIdLo != null
                    && (topicIdHi != 0 || topicIdLo != 0), "Kafka source topic UUID is missing or zero");
            Preconditions.checkArgument(partition != null && partition >= 0, "Kafka source partition is invalid");
            Preconditions.checkArgument(ranges != null && !ranges.isEmpty(), "Kafka source offset ranges are empty");
            OffsetRange previous = null;
            for (OffsetRange range : ranges) {
                Preconditions.checkArgument(range != null, "Kafka source offset range is null");
                range.validate();
                Preconditions.checkArgument(previous == null
                        || (range.first > previous.last && range.first - previous.last > 1),
                        "Kafka source offset ranges must be sorted, disjoint, and nonadjacent");
                previous = range;
            }
        }

        @Override
        public void gsonPostProcess() throws IOException {
            try {
                validate();
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid Kafka source partition", e);
            }
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Partition)) {
                return false;
            }
            Partition sourcePartition = (Partition) other;
            return Objects.equals(topicIdHi, sourcePartition.topicIdHi)
                    && Objects.equals(topicIdLo, sourcePartition.topicIdLo)
                    && Objects.equals(partition, sourcePartition.partition)
                    && Objects.equals(ranges, sourcePartition.ranges);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicIdHi, topicIdLo, partition, ranges);
        }
    }

    public static final class OffsetRange implements GsonPostProcessable {
        @SerializedName("first")
        private final Long first;
        @SerializedName("last")
        private final Long last;

        public OffsetRange(long first, long last) {
            this.first = first;
            this.last = last;
            validate();
        }

        public long getFirst() {
            return first;
        }

        public long getLast() {
            return last;
        }

        private void validate() {
            Preconditions.checkArgument(first != null && last != null && first >= 0 && last >= first,
                    "Invalid Kafka source offset range");
        }

        @Override
        public void gsonPostProcess() throws IOException {
            try {
                validate();
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid Kafka source offset range", e);
            }
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof OffsetRange)) {
                return false;
            }
            OffsetRange range = (OffsetRange) other;
            return Objects.equals(first, range.first) && Objects.equals(last, range.last);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first, last);
        }
    }
}
