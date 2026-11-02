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
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.OffsetRange;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.Partition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
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

public class KafkaSourceCompletionLedgerTest {
    private static final long JOB_ID = 100;
    private static final String CLUSTER_ID = "cluster-é";
    private static final long IDENTITY_BYTES = 28L + CLUSTER_ID.getBytes(StandardCharsets.UTF_8).length;

    private OffsetRange range(long first, long last) {
        return new OffsetRange(first, last);
    }

    private Partition partition(int partition, OffsetRange... ranges) {
        return new Partition(7, 9, partition, Arrays.asList(ranges));
    }

    private KafkaSourceDescriptor descriptor(Partition... partitions) {
        return new KafkaSourceDescriptor(JOB_ID, CLUSTER_ID, Arrays.asList(partitions));
    }

    private KafkaSourceDescriptor descriptor(OffsetRange... ranges) {
        return descriptor(partition(0, ranges));
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

    private void assertCounts(KafkaSourceCompletionLedger ledger, long partitions, long ranges, long identityBytes,
            long pending, long reservedRanges) {
        Assertions.assertEquals(partitions, ledger.getSourcePartitionCount());
        Assertions.assertEquals(ranges, ledger.getOffsetRangeCount());
        Assertions.assertEquals(identityBytes, ledger.getIdentityBytes());
        Assertions.assertEquals(pending, ledger.getPendingTransactionCount());
        Assertions.assertEquals(reservedRanges, ledger.getReservedOffsetRangeCount());
    }

    @Test
    public void testReservationsPublishOnlyCommittedFactsAndResolveIdempotently() throws Exception {
        KafkaSourceCompletionLedger ledger = ledger();
        KafkaSourceDescriptor source = descriptor(range(0, 0), range(2, 4));
        Reservation reservation = ledger.reserve(1, source);
        Assertions.assertSame(reservation, ledger.reserve(1, descriptor(range(0, 0), range(2, 4))));
        Assertions.assertThrows(IllegalStateException.class,
                () -> ledger.reserve(1, descriptor(range(0, 0), range(3, 4))));
        Assertions.assertFalse(ledger.hasCompletions());
        Assertions.assertFalse(ledger.getCompleted(source).isPresent());
        Assertions.assertTrue(ledger.snapshot().isEmpty());
        assertCounts(ledger, 0, 0, 0, 1, 2);

        ledger.publishCommitted(reservation);
        Assertions.assertTrue(ledger.hasCompletions());
        Assertions.assertEquals(Optional.of(source), ledger.getCompleted(source));
        ledger.publishCommitted(reservation);
        ledger.releaseUncommitted(reservation);
        ledger.releaseUncommitted(reservation);
        assertCounts(ledger, 1, 2, IDENTITY_BYTES, 0, 0);

        KafkaSourceDescriptor cancelled = descriptor(range(7, 7));
        Reservation released = ledger.reserve(2, cancelled);
        ledger.releaseUncommitted(released);
        ledger.releaseUncommitted(released);
        Assertions.assertThrows(IllegalStateException.class, () -> ledger.publishCommitted(released));
        Assertions.assertFalse(ledger.getCompleted(cancelled).isPresent());
        Assertions.assertEquals(Collections.singletonList(source), ledger.snapshot());
        assertCounts(ledger, 1, 2, IDENTITY_BYTES, 0, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"partitions", "ranges", "identityBytes"})
    public void testHeldReservationsEnforceEveryBudgetAtomically(String capacity) throws Exception {
        KafkaSourceCompletionLedger ledger = new KafkaSourceCompletionLedger(
                capacity.equals("partitions") ? 1 : 10, capacity.equals("ranges") ? 2 : 10,
                capacity.equals("identityBytes") ? IDENTITY_BYTES : 10000, 10);
        KafkaSourceDescriptor committed = descriptor(range(0, 0));
        KafkaSourceDescriptor held = descriptor(range(2, 2));
        ledger.replayCommitted(committed);
        Reservation unknownOutcome = ledger.reserve(1, held);
        byte[] before = write(ledger);
        KafkaSourceDescriptor candidate = descriptor(partition(0, range(4, 4)), partition(1, range(6, 6)));

        Assertions.assertThrows(CapacityExceededException.class, () -> ledger.reserve(2, candidate));
        Assertions.assertEquals(Collections.singletonList(committed), ledger.snapshot());
        Assertions.assertFalse(ledger.getCompleted(candidate).isPresent());
        Assertions.assertFalse(ledger.getCompleted(held).isPresent());
        Assertions.assertArrayEquals(before, write(ledger));
        assertCounts(ledger, 1, 1, IDENTITY_BYTES, 1, 1);

        Assertions.assertThrows(IllegalStateException.class,
                () -> ledger.replayCommitted(1, descriptor(range(3, 3))));
        Assertions.assertArrayEquals(before, write(ledger));
        Assertions.assertFalse(ledger.getCompleted(held).isPresent());
        assertCounts(ledger, 1, 1, IDENTITY_BYTES, 1, 1);
        ledger.replayCommitted(1, held);
        ledger.publishCommitted(unknownOutcome);
        ledger.replayCommitted(1, held);
        Assertions.assertEquals(Optional.of(held), ledger.getCompleted(held));
        assertCounts(ledger, 1, 2, IDENTITY_BYTES, 0, 0);
    }

    @Test
    public void testPendingReservationsCannotLendCoalescingCredit() throws Exception {
        KafkaSourceCompletionLedger empty = new KafkaSourceCompletionLedger(1, 1, IDENTITY_BYTES, 4);
        KafkaSourceDescriptor bridge = descriptor(range(1, 1));
        KafkaSourceDescriptor sides = descriptor(range(0, 0), range(2, 2));
        Reservation pendingBridge = empty.reserve(1, bridge);
        Assertions.assertThrows(CapacityExceededException.class, () -> empty.reserve(2, sides));
        empty.releaseUncommitted(pendingBridge);
        Assertions.assertThrows(CapacityExceededException.class, () -> empty.reserve(2, sides));
        Reservation durableBridge = empty.reserve(3, bridge);
        empty.publishCommitted(durableBridge);
        Reservation adjacent = empty.reserve(2, sides);
        assertCounts(empty, 1, 1, IDENTITY_BYTES, 1, 0);
        empty.publishCommitted(adjacent);
        Assertions.assertEquals(Collections.singletonList(descriptor(range(0, 2))), empty.snapshot());

        KafkaSourceCompletionLedger shared = new KafkaSourceCompletionLedger(1, 3, IDENTITY_BYTES, 4);
        shared.replayCommitted(descriptor(range(0, 0), range(2, 2)));
        Reservation first = shared.reserve(1, descriptor(range(1, 1), range(4, 4)));
        assertCounts(shared, 1, 2, IDENTITY_BYTES, 1, 1);
        KafkaSourceDescriptor secondSource = descriptor(range(1, 1), range(6, 6));
        Assertions.assertThrows(CapacityExceededException.class, () -> shared.reserve(2, secondSource));
        shared.publishCommitted(first);
        Reservation second = shared.reserve(2, secondSource);
        assertCounts(shared, 1, 2, IDENTITY_BYTES, 1, 1);
        shared.publishCommitted(second);
        Assertions.assertEquals(Collections.singletonList(descriptor(range(0, 2), range(4, 4), range(6, 6))),
                shared.snapshot());
        assertCounts(shared, 1, 3, IDENTITY_BYTES, 0, 0);
    }

    @Test
    public void testZeroGrowthReservationsStillHaveABoundedPendingCount() throws Exception {
        KafkaSourceCompletionLedger ledger = new KafkaSourceCompletionLedger(1, 2, IDENTITY_BYTES, 2);
        ledger.replayCommitted(descriptor(range(0, 0), range(2, 2)));
        Reservation hole = ledger.reserve(1, descriptor(range(1, 1)));
        Reservation duplicate = ledger.reserve(2, descriptor(range(0, 0)));
        Assertions.assertThrows(CapacityExceededException.class,
                () -> ledger.reserve(3, descriptor(range(2, 2))));
        assertCounts(ledger, 1, 2, IDENTITY_BYTES, 2, 0);
        ledger.releaseUncommitted(duplicate);
        Assertions.assertThrows(CapacityExceededException.class,
                () -> ledger.reserve(4, descriptor(range(4, 4))));

        ledger.publishCommitted(hole);
        assertCounts(ledger, 1, 1, IDENTITY_BYTES, 0, 0);
        Reservation growth = ledger.reserve(4, descriptor(range(4, 4)));
        ledger.publishCommitted(growth);
        Assertions.assertEquals(Collections.singletonList(descriptor(range(0, 2), range(4, 4))), ledger.snapshot());

        KafkaSourceCompletionLedger zero = new KafkaSourceCompletionLedger(0, 0, 0, 0);
        zero.replayCommitted(descriptor(range(0, 0)));
        Assertions.assertThrows(CapacityExceededException.class,
                () -> zero.reserve(1, descriptor(range(0, 0))));
        assertCounts(zero, 1, 1, IDENTITY_BYTES, 0, 0);
    }

    @Test
    public void testPersistenceOmitsReservationsAndMergesCommittedFacts() throws Exception {
        KafkaSourceCompletionLedger original = ledger();
        KafkaSourceDescriptor committed = descriptor(range(0, 0));
        KafkaSourceDescriptor pending = descriptor(range(2, 2));
        original.replayCommitted(committed);
        Reservation reservation = original.reserve(1, pending);
        byte[] image = write(original);
        KafkaSourceCompletionLedger restored = ledger();
        KafkaSourceDescriptor other = descriptor(range(4, 4));
        restored.replayCommitted(other);
        byte[] differentImage = write(restored);
        Assertions.assertThrows(IllegalStateException.class, () -> load(original, differentImage));
        Assertions.assertFalse(original.getCompleted(other).isPresent());
        assertCounts(original, 1, 1, IDENTITY_BYTES, 1, 1);

        load(restored, image);
        load(restored, image);
        Assertions.assertEquals(Collections.singletonList(descriptor(range(0, 0), range(4, 4))), restored.snapshot());
        Assertions.assertFalse(restored.getCompleted(pending).isPresent());
        Assertions.assertThrows(IllegalStateException.class, () -> restored.publishCommitted(reservation));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> restored.snapshot().clear());
        assertCounts(restored, 1, 2, IDENTITY_BYTES, 0, 0);

        original.publishCommitted(reservation);
        Assertions.assertEquals(Optional.of(pending), original.getCompleted(pending));
        Assertions.assertFalse(restored.getCompleted(pending).isPresent());
    }

    @Test
    public void testRecoveryRetainsFactsAboveLoweredLimitsAndResetRequiresNoReservations() throws Exception {
        KafkaSourceCompletionLedger original = ledger();
        KafkaSourceDescriptor sparse = descriptor(partition(0, range(0, 0), range(2, 2), range(4, 4)),
                partition(1, range(0, 0), range(2, 2)));
        original.replayCommitted(sparse);
        byte[] image = write(original);
        KafkaSourceCompletionLedger restored = new KafkaSourceCompletionLedger(0, 0, 0, 2);
        load(restored, image);
        KafkaSourceDescriptor tail = descriptor(partition(2, range(Long.MAX_VALUE, Long.MAX_VALUE)));
        restored.replayCommitted(tail);
        restored.replayCommitted(tail);
        assertCounts(restored, 3, 6, 3 * IDENTITY_BYTES, 0, 0);
        Reservation duplicate = restored.reserve(1, descriptor(range(0, 0)));
        restored.publishCommitted(duplicate);
        Reservation coalescing = restored.reserve(2, descriptor(range(1, 3)));
        restored.publishCommitted(coalescing);
        assertCounts(restored, 3, 4, 3 * IDENTITY_BYTES, 0, 0);
        Assertions.assertEquals(Optional.of(tail), restored.getCompleted(tail));
        Assertions.assertThrows(CapacityExceededException.class,
                () -> restored.reserve(3, descriptor(range(6, 6))));
        Assertions.assertThrows(CapacityExceededException.class,
                () -> restored.reserve(3, descriptor(partition(3, range(0, 0)))));

        List<KafkaSourceDescriptor> beforeReset = restored.snapshot();
        Reservation pending = restored.reserve(4, descriptor(range(0, 0)));
        Assertions.assertThrows(IllegalStateException.class, restored::resetForImageLoad);
        Assertions.assertEquals(beforeReset, restored.snapshot());
        assertCounts(restored, 3, 4, 3 * IDENTITY_BYTES, 1, 0);
        restored.releaseUncommitted(pending);
        restored.resetForImageLoad();
        Assertions.assertFalse(restored.hasCompletions());
        Assertions.assertTrue(restored.snapshot().isEmpty());
        assertCounts(restored, 0, 0, 0, 0, 0);
        load(restored, image);
        Assertions.assertEquals(original.snapshot(), restored.snapshot());
        assertCounts(restored, 2, 5, 2 * IDENTITY_BYTES, 0, 0);
    }

    @Test
    public void testSnapshotIODoesNotHoldLedgerOrIndexLocks() throws Exception {
        KafkaSourceCompletionLedger ledger = ledger();
        KafkaSourceDescriptor committed = descriptor(range(0, 0));
        ledger.replayCommitted(committed);
        Reservation pending = ledger.reserve(1, descriptor(range(2, 2)));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        DataOutput output = Mockito.mock(DataOutput.class);
        Mockito.doAnswer(invocation -> {
            Assertions.assertFalse(Thread.holdsLock(ledger));
            Future<Long> query = executor.submit(() -> {
                Assertions.assertEquals(Optional.of(committed), ledger.getCompleted(committed));
                return ledger.getPendingTransactionCount();
            });
            Assertions.assertEquals(1L, query.get(5, TimeUnit.SECONDS));
            return null;
        }).when(output).writeInt(Mockito.anyInt());
        try {
            ledger.write(output);
            Mockito.verify(output, Mockito.atLeastOnce()).writeInt(Mockito.anyInt());
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            ledger.releaseUncommitted(pending);
        }
    }

    @Test
    public void testConcurrentReservationsCannotOversubscribeCapacity() throws Exception {
        KafkaSourceCompletionLedger ledger = new KafkaSourceCompletionLedger(1, 2, IDENTITY_BYTES, 4);
        ledger.replayCommitted(descriptor(range(0, 0)));
        List<KafkaSourceDescriptor> candidates = Arrays.asList(descriptor(range(2, 2)), descriptor(range(4, 4)));
        CyclicBarrier start = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        List<Future<Reservation>> reservations = new ArrayList<>();
        try {
            for (int worker = 0; worker < 2; worker++) {
                long transactionId = worker + 1;
                KafkaSourceDescriptor candidate = candidates.get(worker);
                reservations.add(executor.submit(() -> {
                    start.await(5, TimeUnit.SECONDS);
                    try {
                        return ledger.reserve(transactionId, candidate);
                    } catch (CapacityExceededException e) {
                        return null;
                    }
                }));
            }
            List<Reservation> resolved = new ArrayList<>();
            for (Future<Reservation> reservation : reservations) {
                resolved.add(reservation.get(5, TimeUnit.SECONDS));
            }
            Assertions.assertEquals(1, resolved.stream().filter(ticket -> ticket != null).count());
            assertCounts(ledger, 1, 1, IDENTITY_BYTES, 1, 1);
            for (int worker = 0; worker < 2; worker++) {
                Assertions.assertFalse(ledger.getCompleted(candidates.get(worker)).isPresent());
                Reservation reservation = resolved.get(worker);
                if (reservation != null) {
                    ledger.publishCommitted(reservation);
                    Assertions.assertEquals(Optional.of(candidates.get(worker)),
                            ledger.getCompleted(candidates.get(worker)));
                }
            }
            assertCounts(ledger, 1, 2, IDENTITY_BYTES, 0, 0);
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private KafkaSourceDescriptor descriptorForMask(int mask) {
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
        return descriptor(ranges.toArray(new OffsetRange[0]));
    }

    private Reservation reserveMask(KafkaSourceCompletionLedger ledger, long transactionId, int mask) {
        if (mask == 0) {
            return null;
        }
        try {
            return ledger.reserve(transactionId, descriptorForMask(mask));
        } catch (CapacityExceededException e) {
            return null;
        }
    }

    private void assertCompletedMask(KafkaSourceCompletionLedger ledger, int mask, long allowedRanges, String context) {
        Optional<KafkaSourceDescriptor> expected = mask == 0 ? Optional.empty() : Optional.of(descriptorForMask(mask));
        Assertions.assertEquals(expected, ledger.getCompleted(descriptor(range(0, 6))), context);
        Assertions.assertEquals(mask == 0 ? Collections.emptyList()
                : Collections.singletonList(descriptorForMask(mask)), ledger.snapshot(), context);
        Assertions.assertEquals(Integer.bitCount(mask & ~(mask << 1)), ledger.getOffsetRangeCount(), context);
        Assertions.assertTrue(ledger.getOffsetRangeCount() <= allowedRanges, context);
    }

    @Test
    public void testFiniteSetOracleForReservationPublishAndCancelOrders() {
        Random random = new Random(0x5eedc0deL);
        for (int sample = 0; sample < 128; sample++) {
            int completed = random.nextInt(128);
            int[] incoming = {random.nextInt(128), random.nextInt(128)};
            int quota = sample % 3 + 1;
            long allowedRanges = Math.max(Integer.bitCount(completed & ~(completed << 1)), quota);
            for (int outcomes = 0; outcomes < 4; outcomes++) {
                for (int reverse = 0; reverse < 2; reverse++) {
                    KafkaSourceCompletionLedger ledger = new KafkaSourceCompletionLedger(10, quota,
                            10 * IDENTITY_BYTES, 2);
                    if (completed != 0) {
                        ledger.replayCommitted(descriptorForMask(completed));
                    }
                    Reservation[] reservations = {reserveMask(ledger, 1, incoming[0]),
                            reserveMask(ledger, 2, incoming[1])};
                    int published = completed;
                    String context = "sample=" + sample + ", completed=" + completed
                            + ", incoming=" + Arrays.toString(incoming) + ", quota=" + quota
                            + ", outcomes=" + outcomes + ", reverse=" + reverse;
                    assertCompletedMask(ledger, published, allowedRanges, context);
                    for (int step = 0; step < 2; step++) {
                        int position = reverse == 0 ? step : 1 - step;
                        Reservation reservation = reservations[position];
                        if (reservation != null) {
                            if ((outcomes & (1 << position)) != 0) {
                                ledger.publishCommitted(reservation);
                                published |= incoming[position];
                            } else {
                                ledger.releaseUncommitted(reservation);
                            }
                        }
                        assertCompletedMask(ledger, published, allowedRanges, context);
                    }
                    assertCounts(ledger, published == 0 ? 0 : 1,
                            Integer.bitCount(published & ~(published << 1)), published == 0 ? 0 : IDENTITY_BYTES,
                            0, 0);
                }
            }
        }
    }
}
