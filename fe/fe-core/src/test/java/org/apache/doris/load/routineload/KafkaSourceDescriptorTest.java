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

package org.apache.doris.load.routineload;

import org.apache.doris.cloud.proto.Cloud.KafkaSourceDescriptorPB;
import org.apache.doris.cloud.proto.Cloud.KafkaSourceOffsetRangePB;
import org.apache.doris.cloud.proto.Cloud.TxnCommitAttachmentPB;
import org.apache.doris.cloud.transaction.TxnUtil;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.OffsetRange;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.Partition;
import org.apache.doris.load.routineload.kinesis.KinesisProgress;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TKafkaSourceDescriptor;
import org.apache.doris.thrift.TKafkaSourceOffsetRange;
import org.apache.doris.thrift.TKafkaSourcePartition;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TRLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class KafkaSourceDescriptorTest {
    private static final long JOB_ID = 100;

    private TKafkaSourceDescriptor thriftDescriptor() {
        return new TKafkaSourceDescriptor().setNamespaceId(JOB_ID).setClusterId("cluster-a")
                .setPartitions(Collections.singletonList(new TKafkaSourcePartition()
                        .setTopicId(new TUniqueId(7, 9)).setPartition(0)
                        .setRanges(Arrays.asList(new TKafkaSourceOffsetRange().setFirst(1).setLast(1),
                                new TKafkaSourceOffsetRange().setFirst(3).setLast(4),
                                new TKafkaSourceOffsetRange().setFirst(Long.MAX_VALUE).setLast(Long.MAX_VALUE)))));
    }

    private TRLTaskTxnCommitAttachment thriftAttachment() {
        return new TRLTaskTxnCommitAttachment().setLoadSourceType(TLoadSourceType.KAFKA)
                .setId(new TUniqueId(1, 2)).setJobId(JOB_ID)
                .setKafkaRLTaskProgress(new TKafkaRLTaskProgress(Collections.emptyMap()))
                .setKafkaSourceDescriptor(thriftDescriptor());
    }

    private Partition partition(long hi, long lo, int id, OffsetRange... ranges) {
        return new Partition(hi, lo, id, Arrays.asList(ranges));
    }

    @Test
    public void testSparseRangesAreCopiedAndImmutable() {
        TKafkaSourceDescriptor thrift = thriftDescriptor();
        KafkaSourceDescriptor descriptor = KafkaSourceDescriptor.fromThrift(thrift);
        thrift.getPartitions().get(0).getRanges().get(0).setLast(2);
        thrift.getPartitions().get(0).getTopicId().setHi(11);

        Assertions.assertEquals(7, descriptor.getPartitions().get(0).getTopicIdHi());
        Assertions.assertEquals(Arrays.asList(new OffsetRange(1, 1), new OffsetRange(3, 4),
                new OffsetRange(Long.MAX_VALUE, Long.MAX_VALUE)), descriptor.getPartitions().get(0).getRanges());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> descriptor.getPartitions().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> descriptor.getPartitions().get(0).getRanges().clear());

        List<OffsetRange> ranges = new ArrayList<>(Collections.singletonList(new OffsetRange(0, 0)));
        Partition sourcePartition = new Partition(1, 1, 0, ranges);
        ranges.clear();
        Assertions.assertEquals(1, sourcePartition.getRanges().size());
    }

    @Test
    public void testUnsignedTopicOrderingAndDistinctIncarnations() {
        List<Partition> partitions = Arrays.asList(partition(0, 1, 0, new OffsetRange(1, 1)),
                partition(0, 1, 1, new OffsetRange(1, 1)), partition(0, -1, 0, new OffsetRange(1, 1)),
                partition(-1, 0, 0, new OffsetRange(1, 1)));
        KafkaSourceDescriptor descriptor = new KafkaSourceDescriptor(JOB_ID, "cluster-a", partitions);
        Assertions.assertEquals(4, descriptor.getPartitions().size());
        List<Partition> reversed = new ArrayList<>(partitions);
        Collections.reverse(reversed);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new KafkaSourceDescriptor(JOB_ID, "cluster-a", reversed));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new KafkaSourceDescriptor(JOB_ID, "cluster-a",
                        Arrays.asList(partitions.get(0), partitions.get(0))));
    }

    @Test
    public void testInvalidRangesAndIdentifiersAreRejected() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new OffsetRange(-1, 0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new OffsetRange(2, 1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> partition(1, 1, 0, new OffsetRange(1, 2), new OffsetRange(2, 3)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> partition(1, 1, 0, new OffsetRange(1, 2), new OffsetRange(3, 4)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> partition(1, 1, 0, new OffsetRange(4, 4), new OffsetRange(1, 1)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> partition(0, 0, 0, new OffsetRange(0, 0)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> partition(1, 1, -1, new OffsetRange(0, 0)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> partition(1, 1, 0));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> KafkaSourceDescriptor.fromThrift(thriftDescriptor().setNamespaceId(0)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> KafkaSourceDescriptor.fromThrift(thriftDescriptor().setClusterId(" ")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> KafkaSourceDescriptor.fromThrift(thriftDescriptor().setPartitions(Collections.emptyList())));
    }

    @Test
    public void testMissingWireFieldsAreRejected() {
        TKafkaSourceDescriptor thrift = thriftDescriptor();
        thrift.getPartitions().get(0).getRanges().get(0).unsetFirst();
        Assertions.assertThrows(IllegalArgumentException.class, () -> KafkaSourceDescriptor.fromThrift(thrift));

        KafkaSourceDescriptorPB proto = KafkaSourceDescriptor.fromThrift(thriftDescriptor()).toPb();
        KafkaSourceOffsetRangePB incompleteRange = proto.getPartitions(0).getRanges(0).toBuilder().clearLast().build();
        KafkaSourceDescriptorPB incomplete = proto.toBuilder().setPartitions(0,
                proto.getPartitions(0).toBuilder().setRanges(0, incompleteRange)).build();
        Assertions.assertThrows(IllegalArgumentException.class, () -> KafkaSourceDescriptor.fromPb(incomplete));
    }

    @Test
    public void testCloudAttachmentRoundTrip() {
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment(thriftAttachment());
        TxnCommitAttachmentPB proto = TxnUtil.rlTaskTxnCommitAttachmentToPb(attachment);
        Assertions.assertTrue(proto.getRlTaskTxnCommitAttachment().hasKafkaSourceDescriptor());
        RLTaskTxnCommitAttachment restored = TxnUtil.rtTaskTxnCommitAttachmentFromPb(proto);
        Assertions.assertEquals(attachment.getKafkaSourceDescriptor(), restored.getKafkaSourceDescriptor());
        Assertions.assertEquals(attachment.getKafkaSourceDescriptor().hashCode(),
                restored.getKafkaSourceDescriptor().hashCode());
    }

    @Test
    public void testNamespaceMustMatchAttachmentJob() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new RLTaskTxnCommitAttachment(thriftAttachment().setJobId(JOB_ID + 1)));
        TxnCommitAttachmentPB proto = TxnUtil.rlTaskTxnCommitAttachmentToPb(
                new RLTaskTxnCommitAttachment(thriftAttachment()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new RLTaskTxnCommitAttachment(proto.getRlTaskTxnCommitAttachment().toBuilder()
                        .setJobId(JOB_ID + 1).build()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new RLTaskTxnCommitAttachment(thriftAttachment().setLoadSourceType(TLoadSourceType.KINESIS)));
    }

    @Test
    public void testAbsentDescriptorPreservesLegacyAttachments() {
        TRLTaskTxnCommitAttachment thrift = thriftAttachment();
        thrift.unsetKafkaSourceDescriptor();
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment(thrift);
        Assertions.assertNull(attachment.getKafkaSourceDescriptor());
        TxnCommitAttachmentPB proto = TxnUtil.rlTaskTxnCommitAttachmentToPb(attachment);
        Assertions.assertFalse(proto.getRlTaskTxnCommitAttachment().hasKafkaSourceDescriptor());
        Assertions.assertNull(TxnUtil.rtTaskTxnCommitAttachmentFromPb(proto).getKafkaSourceDescriptor());
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(attachment)).getAsJsonObject();
        json.remove("jobId");
        Assertions.assertNull(GsonUtils.GSON.fromJson(json, RLTaskTxnCommitAttachment.class)
                .getKafkaSourceDescriptor());
    }

    @Test
    public void testTransactionStatePersistenceRetainsDescriptorAndNamespace() throws IOException {
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment(thriftAttachment());
        TransactionState transaction = new TransactionState(1, Collections.singletonList(2L), 3, "label",
                new TUniqueId(4, 5), LoadJobSourceType.ROUTINE_LOAD_TASK,
                new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", 0), TransactionStatus.COMMITTED, "",
                JOB_ID, 1000, attachment, 1, 2, 3, 4);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(buffer)) {
            transaction.write(output);
        }
        TransactionState restored;
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
            restored = TransactionState.read(input);
        }
        RLTaskTxnCommitAttachment restoredAttachment = (RLTaskTxnCommitAttachment) restored.getTxnCommitAttachment();
        Assertions.assertEquals(JOB_ID, restoredAttachment.getJobId());
        Assertions.assertEquals(attachment.getKafkaSourceDescriptor(), restoredAttachment.getKafkaSourceDescriptor());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> restoredAttachment.getKafkaSourceDescriptor().getPartitions().clear());
    }

    @Test
    public void testMalformedPersistedDescriptorIsRejected() {
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment(thriftAttachment());
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(attachment)).getAsJsonObject();
        json.addProperty("jobId", JOB_ID + 1);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> GsonUtils.GSON.fromJson(json, RLTaskTxnCommitAttachment.class));

        JsonObject descriptor = JsonParser.parseString(GsonUtils.GSON.toJson(attachment.getKafkaSourceDescriptor()))
                .getAsJsonObject();
        descriptor.getAsJsonArray("partitions").get(0).getAsJsonObject().getAsJsonArray("ranges")
                .get(0).getAsJsonObject().remove("first");
        Assertions.assertThrows(com.google.gson.JsonParseException.class,
                () -> GsonUtils.GSON.fromJson(descriptor, KafkaSourceDescriptor.class));
        Assertions.assertTrue(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(attachment), TxnCommitAttachment.class)
                instanceof RLTaskTxnCommitAttachment);
    }

    @Test
    public void testPersistedDescriptorRequiresKafkaProgress() {
        String attachment = GsonUtils.GSON.toJson(new RLTaskTxnCommitAttachment(thriftAttachment()));
        JsonObject kinesisProgress = JsonParser.parseString(attachment).getAsJsonObject();
        kinesisProgress.add("pro", GsonUtils.GSON.toJsonTree(new KinesisProgress()));
        JsonObject nullProgress = JsonParser.parseString(attachment).getAsJsonObject();
        nullProgress.add("pro", JsonNull.INSTANCE);
        JsonObject missingProgress = JsonParser.parseString(attachment).getAsJsonObject();
        missingProgress.remove("pro");

        for (JsonObject json : Arrays.asList(kinesisProgress, nullProgress, missingProgress)) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> GsonUtils.GSON.fromJson(json.deepCopy(), RLTaskTxnCommitAttachment.class));
            json.remove("kafkaSourceDescriptor");
            Assertions.assertNull(GsonUtils.GSON.fromJson(json, RLTaskTxnCommitAttachment.class)
                    .getKafkaSourceDescriptor());
        }
    }
}
