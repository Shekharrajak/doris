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

import org.apache.doris.common.io.Text;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionIndex;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionIndex.CapacityExceededException;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.OffsetRange;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.Partition;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaSourceCompletionIndexTest {
    private static final long JOB_ID = 100;
    private static final String CLUSTER_ID = "cluster-é";
    private static final long IDENTITY_BYTES = 28L + CLUSTER_ID.getBytes(StandardCharsets.UTF_8).length;

    private OffsetRange range(long first, long last) {
        return new OffsetRange(first, last);
    }

    private Partition partition(long hi, long lo, int partition, OffsetRange... ranges) {
        return new Partition(hi, lo, partition, Arrays.asList(ranges));
    }

    private KafkaSourceDescriptor descriptor(long namespace, String cluster, Partition... partitions) {
        return new KafkaSourceDescriptor(namespace, cluster, Arrays.asList(partitions));
    }

    private KafkaSourceDescriptor descriptor(OffsetRange... ranges) {
        return descriptor(JOB_ID, CLUSTER_ID, partition(7, 9, 0, ranges));
    }

    private KafkaSourceCompletionIndex index() {
        return new KafkaSourceCompletionIndex(100, 100, 10000);
    }

    private byte[] write(KafkaSourceCompletionIndex index) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            index.write(output);
        }
        return bytes.toByteArray();
    }

    private byte[] writeJson(String json) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            Text.writeString(output, json);
        }
        return bytes.toByteArray();
    }

    private KafkaSourceCompletionIndex read(byte[] bytes, int partitions, int ranges, long identityBytes)
            throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return KafkaSourceCompletionIndex.read(input, partitions, ranges, identityBytes);
        }
    }

    private void assertCounts(KafkaSourceCompletionIndex index, long partitions, long ranges, long identityBytes) {
        Assertions.assertEquals(partitions, index.getSourcePartitionCount());
        Assertions.assertEquals(ranges, index.getOffsetRangeCount());
        Assertions.assertEquals(identityBytes, index.getIdentityBytes());
    }

    @Test
    public void testSparseIntersectionPreservesHolesAndQueryBounds() throws Exception {
        KafkaSourceCompletionIndex index = index();
        KafkaSourceDescriptor stored = descriptor(range(0, 0), range(2, 4), range(8, 9));
        KafkaSourceDescriptor query = descriptor(JOB_ID, CLUSTER_ID,
                partition(7, 9, 0, range(1, 3), range(6, 8)), partition(7, 9, 1, range(0, 9)));
        Assertions.assertFalse(index.getCompleted(query).isPresent());
        index.add(stored);

        Assertions.assertEquals(Optional.of(descriptor(range(2, 3), range(8, 8))), index.getCompleted(query));
        Assertions.assertFalse(index.getCompleted(descriptor(range(1, 1), range(5, 7))).isPresent());
        Assertions.assertEquals(Collections.singletonList(stored), index.snapshot());
        assertCounts(index, 1, 3, IDENTITY_BYTES);
    }

    @Test
    public void testDuplicateOverlapAndAdjacencyCoalesceAtCapacity() throws Exception {
        KafkaSourceCompletionIndex index = new KafkaSourceCompletionIndex(1, 2, IDENTITY_BYTES);
        index.add(descriptor(range(2, 4)));
        index.add(descriptor(range(2, 4)));
        index.add(descriptor(range(3, 6)));
        index.add(descriptor(range(0, 1)));
        index.add(descriptor(range(8, 8)));
        assertCounts(index, 1, 2, IDENTITY_BYTES);
        Assertions.assertEquals(Optional.of(descriptor(range(0, 6), range(8, 8))),
                index.getCompleted(descriptor(range(0, 8))));

        index.add(descriptor(range(7, 7)));
        Assertions.assertEquals(Collections.singletonList(descriptor(range(0, 8))), index.snapshot());
        assertCounts(index, 1, 1, IDENTITY_BYTES);

        KafkaSourceCompletionIndex balanced = new KafkaSourceCompletionIndex(2, 3, 2 * IDENTITY_BYTES);
        balanced.add(descriptor(JOB_ID, CLUSTER_ID, partition(7, 9, 0, range(0, 0)),
                partition(7, 9, 1, range(0, 0), range(2, 2))));
        balanced.add(descriptor(JOB_ID, CLUSTER_ID, partition(7, 9, 0, range(2, 2)),
                partition(7, 9, 1, range(1, 1))));
        Assertions.assertEquals(Collections.singletonList(descriptor(JOB_ID, CLUSTER_ID,
                partition(7, 9, 0, range(0, 0), range(2, 2)), partition(7, 9, 1, range(0, 2)))),
                balanced.snapshot());
        assertCounts(balanced, 2, 3, 2 * IDENTITY_BYTES);
    }

    @Test
    public void testEverySourceIdentityComponentIsIsolated() throws Exception {
        KafkaSourceCompletionIndex index = index();
        List<KafkaSourceDescriptor> sources = Arrays.asList(descriptor(range(7, 7)),
                descriptor(JOB_ID + 1, CLUSTER_ID, partition(7, 9, 0, range(7, 7))),
                descriptor(JOB_ID, "other-cluster", partition(7, 9, 0, range(7, 7))),
                descriptor(JOB_ID, CLUSTER_ID, partition(8, 9, 0, range(7, 7))),
                descriptor(JOB_ID, CLUSTER_ID, partition(7, 10, 0, range(7, 7))),
                descriptor(JOB_ID, CLUSTER_ID, partition(7, 9, 1, range(7, 7))),
                descriptor(JOB_ID, CLUSTER_ID, partition(0, -1, 0, range(7, 7))),
                descriptor(JOB_ID, CLUSTER_ID, partition(-1, 0, 0, range(7, 7))));
        for (KafkaSourceDescriptor source : sources) {
            Assertions.assertFalse(index.getCompleted(source).isPresent());
            index.add(source);
        }
        for (KafkaSourceDescriptor source : sources) {
            Assertions.assertEquals(Optional.of(source), index.getCompleted(source));
        }
        assertCounts(index, 8, 8, 7 * IDENTITY_BYTES + 28
                + "other-cluster".getBytes(StandardCharsets.UTF_8).length);
        Assertions.assertEquals(3, index.snapshot().size());
    }

    @Test
    public void testOffsetExtremesRemainCompressedIntervals() throws Exception {
        KafkaSourceCompletionIndex index = new KafkaSourceCompletionIndex(1, 2, IDENTITY_BYTES);
        index.add(descriptor(range(0, 0), range(Long.MAX_VALUE, Long.MAX_VALUE)));
        assertCounts(index, 1, 2, IDENTITY_BYTES);
        Assertions.assertFalse(index.getCompleted(descriptor(range(1, Long.MAX_VALUE - 1))).isPresent());

        index.add(descriptor(range(1, Long.MAX_VALUE - 1)));
        KafkaSourceDescriptor full = descriptor(range(0, Long.MAX_VALUE));
        Assertions.assertEquals(Collections.singletonList(full), index.snapshot());
        Assertions.assertEquals(Optional.of(full), index.getCompleted(full));
        Assertions.assertEquals(Optional.of(descriptor(range(Long.MAX_VALUE, Long.MAX_VALUE))),
                index.getCompleted(descriptor(range(Long.MAX_VALUE, Long.MAX_VALUE))));
        index.add(full);
        assertCounts(index, 1, 1, IDENTITY_BYTES);
    }

    @ParameterizedTest
    @ValueSource(strings = {"partitions", "ranges", "identityBytes"})
    public void testCapacityFailureIsAtomicAcrossPartitions(String capacity) throws Exception {
        KafkaSourceCompletionIndex index = new KafkaSourceCompletionIndex(capacity.equals("partitions") ? 1 : 10,
                capacity.equals("ranges") ? 2 : 10, capacity.equals("identityBytes") ? IDENTITY_BYTES : 10000);
        KafkaSourceDescriptor original = descriptor(range(0, 0));
        index.add(original);
        byte[] before = write(index);
        KafkaSourceDescriptor incoming = descriptor(JOB_ID, CLUSTER_ID,
                partition(7, 9, 0, range(2, 2)), partition(7, 9, 1, range(4, 4)));

        Assertions.assertThrows(CapacityExceededException.class, () -> index.add(incoming));
        Assertions.assertEquals(Collections.singletonList(original), index.snapshot());
        Assertions.assertArrayEquals(before, write(index));
        Assertions.assertFalse(index.getCompleted(incoming).isPresent());
        assertCounts(index, 1, 1, IDENTITY_BYTES);
    }

    @Test
    public void testSnapshotsAreImmutableDeterministicAndRoundTrip() throws Exception {
        List<KafkaSourceDescriptor> sources = Arrays.asList(
                descriptor(JOB_ID + 1, CLUSTER_ID, partition(7, 9, 0, range(0, 2))),
                descriptor(JOB_ID, "other-cluster", partition(7, 9, 0, range(3, 5))),
                descriptor(JOB_ID, CLUSTER_ID, partition(-1, 0, 0, range(6, 8))),
                descriptor(JOB_ID, CLUSTER_ID, partition(0, -1, 0, range(9, 11))),
                descriptor(JOB_ID, CLUSTER_ID, partition(7, 9, 1, range(12, 14))));
        KafkaSourceCompletionIndex forward = index();
        KafkaSourceCompletionIndex reverse = index();
        for (KafkaSourceDescriptor source : sources) {
            forward.add(source);
        }
        List<KafkaSourceDescriptor> reversed = new ArrayList<>(sources);
        Collections.reverse(reversed);
        for (KafkaSourceDescriptor source : reversed) {
            reverse.add(source);
        }

        List<KafkaSourceDescriptor> snapshot = forward.snapshot();
        Assertions.assertEquals(snapshot, reverse.snapshot());
        Assertions.assertArrayEquals(write(forward), write(reverse));
        Assertions.assertThrows(UnsupportedOperationException.class, snapshot::clear);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> snapshot.get(0).getPartitions().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> snapshot.get(0).getPartitions().get(0).getRanges().clear());

        KafkaSourceCompletionIndex restored = read(write(forward), 100, 100, 10000);
        Assertions.assertEquals(snapshot, restored.snapshot());
        Assertions.assertArrayEquals(write(forward), write(restored));
        assertCounts(restored, forward.getSourcePartitionCount(), forward.getOffsetRangeCount(),
                forward.getIdentityBytes());
        forward.add(descriptor(range(20, 20)));
        Assertions.assertEquals(snapshot, restored.snapshot());
        Assertions.assertNotEquals(snapshot, forward.snapshot());
    }

    @Test
    public void testRestoreAboveLoweredQuotaRetainsFactsAndAllowsNoGrowth() throws Exception {
        KafkaSourceCompletionIndex original = index();
        KafkaSourceDescriptor sparse = descriptor(JOB_ID, CLUSTER_ID,
                partition(7, 9, 0, range(0, 0), range(2, 2), range(4, 4)),
                partition(7, 9, 1, range(0, 0), range(2, 2)));
        original.add(sparse);
        KafkaSourceCompletionIndex restored = read(write(original), 1, 1, IDENTITY_BYTES);
        Assertions.assertEquals(original.snapshot(), restored.snapshot());
        assertCounts(restored, 2, 5, 2 * IDENTITY_BYTES);
        restored.add(sparse);

        restored.add(descriptor(JOB_ID, CLUSTER_ID,
                partition(7, 9, 0, range(1, 3)), partition(7, 9, 1, range(1, 1))));
        assertCounts(restored, 2, 2, 2 * IDENTITY_BYTES);
        restored.add(descriptor(range(5, 9)));
        KafkaSourceDescriptor retained = descriptor(JOB_ID, CLUSTER_ID,
                partition(7, 9, 0, range(0, 9)), partition(7, 9, 1, range(0, 2)));
        Assertions.assertEquals(Collections.singletonList(retained), restored.snapshot());

        Assertions.assertThrows(CapacityExceededException.class, () -> restored.add(descriptor(range(11, 11))));
        Assertions.assertThrows(CapacityExceededException.class, () -> restored.add(descriptor(JOB_ID, CLUSTER_ID,
                partition(7, 9, 2, range(0, 0)))));
        Assertions.assertEquals(Collections.singletonList(retained), restored.snapshot());
        Assertions.assertEquals(restored.snapshot(), read(write(restored), 1, 1, IDENTITY_BYTES).snapshot());
        assertCounts(restored, 2, 2, 2 * IDENTITY_BYTES);
    }

    @Test
    public void testMalformedAndUnknownVersionPersistenceIsRejected() throws Exception {
        KafkaSourceCompletionIndex index = index();
        index.add(descriptor(range(0, 2)));
        byte[] bytes = write(index);
        Assertions.assertThrows(IOException.class,
                () -> read(Arrays.copyOf(bytes, bytes.length - 1), 100, 100, 10000));
        Assertions.assertThrows(IOException.class, () -> read(writeJson("{"), 100, 100, 10000));

        String json;
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            json = Text.readString(input);
        }
        JsonObject unknown = JsonParser.parseString(json).getAsJsonObject();
        unknown.addProperty("version", 2);
        Assertions.assertThrows(IOException.class, () -> read(writeJson(unknown.toString()), 100, 100, 10000));
        JsonObject incomplete = JsonParser.parseString(json).getAsJsonObject();
        incomplete.getAsJsonArray("sources").get(0).getAsJsonObject().remove("namespaceId");
        Assertions.assertThrows(IOException.class, () -> read(writeJson(incomplete.toString()), 100, 100, 10000));
        JsonObject invalidRange = JsonParser.parseString(json).getAsJsonObject();
        invalidRange.getAsJsonArray("sources").get(0).getAsJsonObject().getAsJsonArray("partitions")
                .get(0).getAsJsonObject().getAsJsonArray("ranges").get(0).getAsJsonObject().addProperty("first", -1);
        Assertions.assertThrows(IOException.class, () -> read(writeJson(invalidRange.toString()), 100, 100, 10000));
    }

    private KafkaSourceDescriptor descriptorForMask(int mask) {
        List<OffsetRange> ranges = new ArrayList<>();
        for (int offset = 0; offset < 5; offset++) {
            if ((mask & (1 << offset)) == 0) {
                continue;
            }
            int first = offset;
            while (offset + 1 < 5 && (mask & (1 << (offset + 1))) != 0) {
                offset++;
            }
            ranges.add(range(first, offset));
        }
        return descriptor(ranges.toArray(new OffsetRange[0]));
    }

    private void assertCompletedMask(KafkaSourceCompletionIndex index, KafkaSourceDescriptor query, int expected) {
        Optional<KafkaSourceDescriptor> completed = index.getCompleted(query);
        if (expected == 0) {
            Assertions.assertFalse(completed.isPresent());
        } else {
            Assertions.assertEquals(Optional.of(descriptorForMask(expected)), completed);
        }
    }

    @Test
    public void testFiniteSetOracleForIntersectionUnionAndRunCounts() throws Exception {
        for (int first = 0; first < 32; first++) {
            for (int second = 0; second < 32; second++) {
                KafkaSourceCompletionIndex index = new KafkaSourceCompletionIndex(1, 3, IDENTITY_BYTES);
                if (first != 0) {
                    index.add(descriptorForMask(first));
                }
                if (second != 0) {
                    KafkaSourceDescriptor query = descriptorForMask(second);
                    assertCompletedMask(index, query, first & second);
                    index.add(query);
                }
                int union = first | second;
                assertCompletedMask(index, descriptor(range(0, 4)), union);
                Assertions.assertEquals(union == 0 ? Collections.emptyList()
                        : Collections.singletonList(descriptorForMask(union)), index.snapshot());
                assertCounts(index, union == 0 ? 0 : 1, Integer.bitCount(union & ~(union << 1)),
                        union == 0 ? 0 : IDENTITY_BYTES);
            }
        }
    }

    @Test
    public void testConcurrentAddsAndQueriesRetainTheCompleteUnion() throws Exception {
        KafkaSourceCompletionIndex index = new KafkaSourceCompletionIndex(1, 4, IDENTITY_BYTES);
        CyclicBarrier start = new CyclicBarrier(4);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<Void>> additions = new ArrayList<>();
        try {
            for (int worker = 0; worker < 4; worker++) {
                KafkaSourceDescriptor source = descriptor(range(worker * 10L, worker * 10L + 9));
                additions.add(executor.submit(() -> {
                    start.await(5, TimeUnit.SECONDS);
                    index.add(source);
                    Assertions.assertEquals(Optional.of(source), index.getCompleted(source));
                    index.snapshot();
                    return null;
                }));
            }
            for (Future<Void> addition : additions) {
                addition.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
        Assertions.assertEquals(Collections.singletonList(descriptor(range(0, 39))), index.snapshot());
        assertCounts(index, 1, 1, IDENTITY_BYTES);
    }
}
