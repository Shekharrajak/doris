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

import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionIndex.CapacityExceededException;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionLedger;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionLedger.Reservation;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionLedger.SourceConflictException;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.OffsetRange;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.Partition;

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
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaSourceExclusiveClaimsTest {
    private static final long JOB_ID = 100;
    private static final String CLUSTER_ID = "cluster-é";
    private static final long IDENTITY_BYTES = 28L + CLUSTER_ID.getBytes(StandardCharsets.UTF_8).length;

    private OffsetRange range(long first, long last) {
        return new OffsetRange(first, last);
    }

    private Partition partition(int partition, OffsetRange... ranges) {
        return new Partition(7, 9, partition, Arrays.asList(ranges));
    }

    private KafkaSourceDescriptor source(Partition... partitions) {
        return new KafkaSourceDescriptor(JOB_ID, CLUSTER_ID, Arrays.asList(partitions));
    }

    private KafkaSourceDescriptor source(OffsetRange... ranges) {
        return source(partition(0, ranges));
    }

    private KafkaSourceCompletionLedger ledger() {
        return new KafkaSourceCompletionLedger(100, 100, 10000, 10);
    }

    private byte[] write(KafkaSourceCompletionLedger ledger) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            ledger.write(output);
        }
        return bytes.toByteArray();
    }

    private void load(KafkaSourceCompletionLedger ledger, byte[] bytes) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            ledger.readFields(input);
            Assertions.assertEquals(0, input.available());
        }
    }

    private void assertClaims(KafkaSourceCompletionLedger ledger, long transactions, long partitions, long ranges,
            long identityBytes) {
        Assertions.assertEquals(transactions, ledger.getPendingTransactionCount());
        Assertions.assertEquals(partitions, ledger.getPendingClaimPartitionCount());
        Assertions.assertEquals(ranges, ledger.getPendingClaimRangeCount());
        Assertions.assertEquals(identityBytes, ledger.getPendingClaimIdentityBytes());
    }

    @Test
    public void testConcurrentOverlapAdmitsOneWholeBatchAndLeavesLoserTailFree() throws Exception {
        KafkaSourceCompletionLedger ledger = ledger();
        List<KafkaSourceDescriptor> sources = Arrays.asList(source(range(1, 2)), source(range(2, 3)));
        CyclicBarrier start = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        List<Future<Reservation>> attempts = new ArrayList<>();
        try {
            for (int worker = 0; worker < 2; worker++) {
                long transactionId = worker + 1;
                KafkaSourceDescriptor candidate = sources.get(worker);
                attempts.add(executor.submit(() -> {
                    start.await(5, TimeUnit.SECONDS);
                    try {
                        return ledger.reserveExclusive(transactionId, candidate);
                    } catch (SourceConflictException e) {
                        return null;
                    }
                }));
            }
            Reservation first = attempts.get(0).get(5, TimeUnit.SECONDS);
            Reservation second = attempts.get(1).get(5, TimeUnit.SECONDS);
            Assertions.assertTrue((first == null) != (second == null));
            Assertions.assertTrue(ledger.snapshot().isEmpty());
            assertClaims(ledger, 1, 1, 1, IDENTITY_BYTES);
            KafkaSourceDescriptor tail = first != null ? source(range(3, 3)) : source(range(1, 1));
            Reservation independent = ledger.reserveExclusive(3, tail);
            assertClaims(ledger, 2, 1, 2, IDENTITY_BYTES);
            ledger.releaseUncommitted(first != null ? first : second);
            Assertions.assertThrows(SourceConflictException.class, () -> ledger.reserveExclusive(4, tail));
            Reservation sharedOffset = ledger.reserveExclusive(5, source(range(2, 2)));
            ledger.releaseUncommitted(sharedOffset);
            ledger.releaseUncommitted(independent);
            assertClaims(ledger, 0, 0, 0, 0);
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testLatePartitionOverlapRejectsWholeBatchWithoutPartialClaims() throws Exception {
        KafkaSourceCompletionLedger ledger = ledger();
        KafkaSourceDescriptor done = source(partition(1, range(20, 20)));
        ledger.replayCommitted(done);
        KafkaSourceDescriptor mixed = source(partition(0, range(10, 10)), partition(1, range(20, 21)));
        byte[] before = write(ledger);
        Assertions.assertThrows(SourceConflictException.class, () -> ledger.reserveExclusive(1, mixed));
        Assertions.assertArrayEquals(before, write(ledger));
        assertClaims(ledger, 0, 0, 0, 0);

        Reservation untouched = ledger.reserveExclusive(2, source(partition(0, range(10, 10))));
        Reservation otherTail = ledger.reserveExclusive(3, source(partition(1, range(21, 21))));
        ledger.releaseUncommitted(untouched);
        ledger.releaseUncommitted(otherTail);
        Assertions.assertEquals(Collections.singletonList(done), ledger.snapshot());
        assertClaims(ledger, 0, 0, 0, 0);

        KafkaSourceCompletionLedger pending = ledger();
        Reservation owner = pending.reserveExclusive(10, done);
        Assertions.assertThrows(SourceConflictException.class, () -> pending.reserveExclusive(11, mixed));
        assertClaims(pending, 1, 1, 1, IDENTITY_BYTES);
        Assertions.assertTrue(pending.snapshot().isEmpty());
        Reservation freshPartition = pending.reserveExclusive(12, source(partition(0, range(10, 10))));
        Reservation pendingTail = pending.reserveExclusive(13, source(partition(1, range(21, 21))));
        assertClaims(pending, 3, 2, 3, 2 * IDENTITY_BYTES);
        pending.releaseUncommitted(owner);
        pending.releaseUncommitted(freshPartition);
        pending.releaseUncommitted(pendingTail);
        assertClaims(pending, 0, 0, 0, 0);
    }

    @Test
    public void testReservationIdentityAndModeCannotChangeAndPublishedClaimsStayCompleted() throws Exception {
        KafkaSourceCompletionLedger ledger = ledger();
        KafkaSourceDescriptor candidate = source(range(1, 2));
        Reservation strict = ledger.reserveExclusive(1, candidate);
        Assertions.assertSame(strict, ledger.reserveExclusive(1, source(range(1, 2))));
        Assertions.assertThrows(IllegalStateException.class, () -> ledger.reserve(1, candidate));
        Assertions.assertThrows(IllegalStateException.class,
                () -> ledger.reserveExclusive(1, source(range(1, 3))));
        Assertions.assertFalse(ledger.getCompleted(candidate).isPresent());
        ledger.publishCommitted(strict);
        ledger.publishCommitted(strict);
        ledger.releaseUncommitted(strict);
        Assertions.assertEquals(Optional.of(candidate), ledger.getCompleted(candidate));
        Assertions.assertThrows(SourceConflictException.class, () -> ledger.reserveExclusive(2, candidate));
        assertClaims(ledger, 0, 0, 0, 0);

        Reservation legacy = ledger.reserve(3, candidate);
        Assertions.assertSame(legacy, ledger.reserve(3, source(range(1, 2))));
        Assertions.assertThrows(IllegalStateException.class, () -> ledger.reserveExclusive(3, candidate));
        Assertions.assertThrows(IllegalStateException.class, () -> ledger.reserve(3, source(range(1, 3))));
        ledger.releaseUncommitted(legacy);
        Reservation cancelled = ledger.reserveExclusive(4, source(range(4, 4)));
        ledger.releaseUncommitted(cancelled);
        ledger.releaseUncommitted(cancelled);
        Assertions.assertThrows(IllegalStateException.class, () -> ledger.publishCommitted(cancelled));
        Reservation replacement = ledger.reserveExclusive(5, source(range(4, 4)));
        ledger.releaseUncommitted(replacement);
        assertClaims(ledger, 0, 0, 0, 0);
    }

    @Test
    public void testLegacyAndExclusiveModesCannotBypassEachOtherWithinANamespace() throws Exception {
        KafkaSourceCompletionLedger ledger = ledger();
        Reservation firstLegacy = ledger.reserve(1, source(range(1, 1)));
        Reservation secondLegacy = ledger.reserve(2, source(range(1, 1)));
        KafkaSourceDescriptor differentCluster = new KafkaSourceDescriptor(JOB_ID, "other-cluster",
                Collections.singletonList(partition(0, range(9, 9))));
        Assertions.assertThrows(SourceConflictException.class,
                () -> ledger.reserveExclusive(3, differentCluster));
        ledger.releaseUncommitted(firstLegacy);
        Assertions.assertThrows(SourceConflictException.class,
                () -> ledger.reserveExclusive(3, differentCluster));
        ledger.releaseUncommitted(secondLegacy);

        Reservation exclusive = ledger.reserveExclusive(3, differentCluster);
        Assertions.assertThrows(IllegalStateException.class, () -> ledger.reserve(4, source(range(5, 5))));
        KafkaSourceDescriptor otherNamespace = new KafkaSourceDescriptor(JOB_ID + 1, CLUSTER_ID,
                Collections.singletonList(partition(0, range(9, 9))));
        Reservation independent = ledger.reserve(5, otherNamespace);
        ledger.releaseUncommitted(exclusive);
        Reservation legacyAgain = ledger.reserve(6, source(range(5, 5)));
        ledger.releaseUncommitted(legacyAgain);
        ledger.releaseUncommitted(independent);
        assertClaims(ledger, 0, 0, 0, 0);
    }

    @Test
    public void testSparseMaximumOffsetsAndSourceIdentityRemainIndependent() throws Exception {
        KafkaSourceCompletionLedger ledger = ledger();
        Reservation edges = ledger.reserveExclusive(1,
                source(range(0, 0), range(2, 2), range(Long.MAX_VALUE, Long.MAX_VALUE)));
        Reservation gaps = ledger.reserveExclusive(2, source(range(1, 1), range(3, Long.MAX_VALUE - 1)));
        List<KafkaSourceDescriptor> scopes = Arrays.asList(
                new KafkaSourceDescriptor(JOB_ID + 1, CLUSTER_ID,
                        Collections.singletonList(partition(0, range(2, 2)))),
                new KafkaSourceDescriptor(JOB_ID, "other-cluster",
                        Collections.singletonList(partition(0, range(2, 2)))),
                source(partition(1, range(2, 2))),
                new KafkaSourceDescriptor(JOB_ID, CLUSTER_ID, Collections.singletonList(
                        new Partition(0, -1, 0, Collections.singletonList(range(2, 2))))),
                new KafkaSourceDescriptor(JOB_ID, CLUSTER_ID, Collections.singletonList(
                        new Partition(-1, 0, 0, Collections.singletonList(range(2, 2))))));
        List<Reservation> independent = new ArrayList<>();
        for (int i = 0; i < scopes.size(); i++) {
            independent.add(ledger.reserveExclusive(i + 3, scopes.get(i)));
        }
        Assertions.assertThrows(SourceConflictException.class,
                () -> ledger.reserveExclusive(9, source(range(Long.MAX_VALUE, Long.MAX_VALUE))));
        ledger.releaseUncommitted(gaps);
        Reservation adjacentToMaximum = ledger.reserveExclusive(10,
                source(range(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1)));
        ledger.releaseUncommitted(edges);
        Assertions.assertThrows(SourceConflictException.class,
                () -> ledger.reserveExclusive(11, source(range(Long.MAX_VALUE - 1, Long.MAX_VALUE - 1))));
        Reservation maximum = ledger.reserveExclusive(12, source(range(Long.MAX_VALUE, Long.MAX_VALUE)));
        ledger.releaseUncommitted(maximum);
        ledger.releaseUncommitted(adjacentToMaximum);
        for (Reservation reservation : independent) {
            ledger.releaseUncommitted(reservation);
        }
        Assertions.assertTrue(ledger.snapshot().isEmpty());
        assertClaims(ledger, 0, 0, 0, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"partitions", "ranges", "identityBytes"})
    public void testZeroCompletionGrowthStillNeedsAtomicClaimCapacity(String capacity) throws Exception {
        KafkaSourceCompletionLedger ledger = new KafkaSourceCompletionLedger(
                capacity.equals("partitions") ? 2 : 10, capacity.equals("ranges") ? 2 : 10,
                capacity.equals("identityBytes") ? 2 * IDENTITY_BYTES : 10000, 10);
        KafkaSourceDescriptor done = source(partition(0, range(0, 0)), partition(1, range(0, 0)),
                partition(2, range(0, 0)));
        ledger.replayCommitted(done);
        Reservation held = ledger.reserveExclusive(1, source(partition(2, range(1, 1))));
        KafkaSourceDescriptor wholeBatch = source(partition(0, range(1, 1)), partition(1, range(1, 1)));
        Assertions.assertThrows(CapacityExceededException.class, () -> ledger.reserveExclusive(2, wholeBatch));
        Assertions.assertEquals(Collections.singletonList(done), ledger.snapshot());
        Assertions.assertEquals(0, ledger.getReservedOffsetRangeCount());
        assertClaims(ledger, 1, 1, 1, IDENTITY_BYTES);

        Reservation firstPartition = ledger.reserveExclusive(3, source(partition(0, range(1, 1))));
        ledger.releaseUncommitted(firstPartition);
        ledger.releaseUncommitted(held);
        Reservation retry = ledger.reserveExclusive(4, wholeBatch);
        assertClaims(ledger, 1, 2, 2, 2 * IDENTITY_BYTES);
        ledger.releaseUncommitted(retry);
        assertClaims(ledger, 0, 0, 0, 0);
    }

    @Test
    public void testMatchingReplayResolvesHeldClaimsAndImagesOmitPending() throws Exception {
        KafkaSourceCompletionLedger ledger = ledger();
        KafkaSourceDescriptor completed = source(range(0, 0));
        KafkaSourceDescriptor uncertain = source(range(2, 3));
        ledger.replayCommitted(completed);
        Reservation owner = ledger.reserveExclusive(1, uncertain);
        byte[] image = write(ledger);
        KafkaSourceCompletionLedger restored = ledger();
        load(restored, image);
        Assertions.assertEquals(Collections.singletonList(completed), restored.snapshot());
        Assertions.assertFalse(restored.getCompleted(uncertain).isPresent());
        assertClaims(restored, 0, 0, 0, 0);

        Assertions.assertThrows(SourceConflictException.class, () -> ledger.reserveExclusive(2, uncertain));
        Assertions.assertThrows(IllegalStateException.class,
                () -> ledger.replayCommitted(1, source(range(2, 2))));
        Assertions.assertArrayEquals(image, write(ledger));
        assertClaims(ledger, 1, 1, 1, IDENTITY_BYTES);
        ledger.replayCommitted(1, uncertain);
        ledger.publishCommitted(owner);
        ledger.replayCommitted(1, uncertain);
        Assertions.assertEquals(Optional.of(uncertain), ledger.getCompleted(uncertain));
        Assertions.assertThrows(SourceConflictException.class, () -> ledger.reserveExclusive(3, uncertain));
        assertClaims(ledger, 0, 0, 0, 0);

        KafkaSourceCompletionLedger lowered = new KafkaSourceCompletionLedger(0, 0, 0, 2);
        load(lowered, write(ledger));
        Assertions.assertEquals(ledger.snapshot(), lowered.snapshot());
        Assertions.assertThrows(SourceConflictException.class, () -> lowered.reserveExclusive(4, uncertain));
        Assertions.assertThrows(CapacityExceededException.class,
                () -> lowered.reserveExclusive(5, source(range(5, 5))));
        assertClaims(lowered, 0, 0, 0, 0);
    }

    private KafkaSourceDescriptor sourceForMask(int mask) {
        List<OffsetRange> ranges = new ArrayList<>();
        for (int offset = 0; offset < 7; offset++) {
            if ((mask & (1 << offset)) == 0) {
                continue;
            }
            int first = offset;
            while (offset + 1 < 7 && (mask & (1 << (offset + 1))) != 0) {
                offset++;
            }
            ranges.add(range(first, offset));
        }
        return source(ranges.toArray(new OffsetRange[0]));
    }

    private Reservation reserveMask(KafkaSourceCompletionLedger ledger, long transactionId, int mask, boolean allowed)
            throws Exception {
        if (mask == 0) {
            return null;
        }
        if (allowed) {
            return ledger.reserveExclusive(transactionId, sourceForMask(mask));
        }
        Assertions.assertThrows(SourceConflictException.class,
                () -> ledger.reserveExclusive(transactionId, sourceForMask(mask)));
        return null;
    }

    private void assertFacts(KafkaSourceCompletionLedger ledger, int mask) {
        Assertions.assertEquals(mask == 0 ? Optional.empty() : Optional.of(sourceForMask(mask)),
                ledger.getCompleted(source(range(0, 6))));
        Assertions.assertEquals(Integer.bitCount(mask & ~(mask << 1)), ledger.getOffsetRangeCount());
    }

    @Test
    public void testFiniteSetOracleForExactOwnershipAndResolutionOrders() throws Exception {
        Random random = new Random(0xe1c1a1L);
        for (int sample = 0; sample < 64; sample++) {
            int completed = sample % 3 == 0 ? 0 : random.nextInt(128);
            int[] incoming = {random.nextInt(128), random.nextInt(128)};
            for (int outcomes = 0; outcomes < 4; outcomes++) {
                for (int reverse = 0; reverse < 2; reverse++) {
                    KafkaSourceCompletionLedger ledger = ledger();
                    if (completed != 0) {
                        ledger.replayCommitted(sourceForMask(completed));
                    }
                    boolean firstAllowed = (completed & incoming[0]) == 0;
                    boolean secondAllowed = (completed & incoming[1]) == 0
                            && (!firstAllowed || (incoming[0] & incoming[1]) == 0);
                    Reservation[] tickets = {reserveMask(ledger, 1, incoming[0], firstAllowed),
                            reserveMask(ledger, 2, incoming[1], secondAllowed)};
                    int facts = completed;
                    assertFacts(ledger, facts);
                    for (int step = 0; step < 2; step++) {
                        int position = reverse == 0 ? step : 1 - step;
                        if (tickets[position] != null) {
                            if ((outcomes & (1 << position)) != 0) {
                                ledger.publishCommitted(tickets[position]);
                                facts |= incoming[position];
                            } else {
                                ledger.releaseUncommitted(tickets[position]);
                            }
                        }
                        assertFacts(ledger, facts);
                    }
                    assertClaims(ledger, 0, 0, 0, 0);
                }
            }
        }
    }
}
