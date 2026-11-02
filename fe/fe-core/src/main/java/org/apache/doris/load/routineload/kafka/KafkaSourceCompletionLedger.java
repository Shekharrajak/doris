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
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionIndex.ReservationCapacity;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class KafkaSourceCompletionLedger {
    private final int maxSourcePartitions;
    private final int maxOffsetRanges;
    private final long maxIdentityBytes;
    private final int maxPendingTransactions;
    private final Map<Long, Reservation> pending = new HashMap<>();
    private volatile KafkaSourceCompletionIndex index;
    private long reservedPartitions;
    private long reservedRanges;
    private long reservedIdentityBytes;

    public KafkaSourceCompletionLedger(int maxSourcePartitions, int maxOffsetRanges, long maxIdentityBytes,
            int maxPendingTransactions) {
        Preconditions.checkArgument(maxPendingTransactions >= 0, "Pending transaction limit must be nonnegative");
        this.maxSourcePartitions = maxSourcePartitions;
        this.maxOffsetRanges = maxOffsetRanges;
        this.maxIdentityBytes = maxIdentityBytes;
        this.maxPendingTransactions = maxPendingTransactions;
        this.index = new KafkaSourceCompletionIndex(maxSourcePartitions, maxOffsetRanges, maxIdentityBytes);
    }

    public synchronized Reservation reserve(long transactionId, KafkaSourceDescriptor descriptor)
            throws CapacityExceededException {
        Preconditions.checkArgument(transactionId > 0, "Kafka source transaction ID must be positive");
        Reservation existing = pending.get(transactionId);
        if (existing != null) {
            Preconditions.checkState(existing.descriptor.equals(descriptor),
                    "Kafka source reservation identity changed for transaction %s", transactionId);
            return existing;
        }
        ReservationCapacity capacity = index.reservationCapacity(descriptor);
        long nextPartitions = Math.addExact(reservedPartitions, capacity.sourcePartitions());
        long nextRanges = Math.addExact(reservedRanges, capacity.offsetRanges());
        long nextIdentityBytes = Math.addExact(reservedIdentityBytes, capacity.identityBytes());
        if (pending.size() >= maxPendingTransactions
                || exceeds(index.getSourcePartitionCount(), nextPartitions, capacity.sourcePartitions(),
                        maxSourcePartitions)
                || exceeds(index.getOffsetRangeCount(), nextRanges, capacity.offsetRanges(), maxOffsetRanges)
                || exceeds(index.getIdentityBytes(), nextIdentityBytes, capacity.identityBytes(), maxIdentityBytes)) {
            throw new CapacityExceededException("Kafka source completion capacity exceeded for transaction "
                    + transactionId + ": pendingTransactions=" + pending.size() + "/" + maxPendingTransactions
                    + ", partitions=" + index.getSourcePartitionCount() + "+" + nextPartitions + "/"
                    + maxSourcePartitions + ", ranges=" + index.getOffsetRangeCount() + "+" + nextRanges + "/"
                    + maxOffsetRanges + ", identityBytes=" + index.getIdentityBytes() + "+" + nextIdentityBytes
                    + "/" + maxIdentityBytes);
        }
        Reservation reservation = new Reservation(this, transactionId, descriptor, capacity);
        pending.put(transactionId, reservation);
        reservedPartitions = nextPartitions;
        reservedRanges = nextRanges;
        reservedIdentityBytes = nextIdentityBytes;
        return reservation;
    }

    private static boolean exceeds(long completed, long reserved, long growth, long limit) {
        return growth > 0 && Math.addExact(completed, reserved) > limit;
    }

    public synchronized void publishCommitted(Reservation reservation) {
        Preconditions.checkState(reservation.owner == this, "Kafka source reservation belongs to another ledger");
        if (reservation.published) {
            return;
        }
        Preconditions.checkState(pending.get(reservation.transactionId) == reservation,
                "Kafka source reservation is not pending");
        index.restore(reservation.descriptor);
        removeReservation(reservation);
        reservation.published = true;
    }

    public synchronized void releaseUncommitted(Reservation reservation) {
        Preconditions.checkState(reservation.owner == this, "Kafka source reservation belongs to another ledger");
        if (reservation.published || reservation.released) {
            return;
        }
        Preconditions.checkState(pending.get(reservation.transactionId) == reservation,
                "Kafka source reservation is not pending");
        removeReservation(reservation);
        reservation.released = true;
    }

    private void removeReservation(Reservation reservation) {
        pending.remove(reservation.transactionId);
        reservedPartitions -= reservation.capacity.sourcePartitions();
        reservedRanges -= reservation.capacity.offsetRanges();
        reservedIdentityBytes -= reservation.capacity.identityBytes();
    }

    public synchronized void replayCommitted(KafkaSourceDescriptor descriptor) {
        index.restore(descriptor);
    }

    public synchronized void replayCommitted(long transactionId, KafkaSourceDescriptor descriptor) {
        Reservation reservation = pending.get(transactionId);
        if (reservation != null) {
            Preconditions.checkState(reservation.descriptor.equals(descriptor),
                    "Replayed Kafka source reservation identity changed for transaction %s", transactionId);
        }
        index.restore(descriptor);
        if (reservation != null) {
            removeReservation(reservation);
            reservation.published = true;
        }
    }

    public synchronized Optional<KafkaSourceDescriptor> getCompleted(KafkaSourceDescriptor query) {
        return index.getCompleted(query);
    }

    public synchronized List<KafkaSourceDescriptor> snapshot() {
        return index.snapshot();
    }

    public synchronized boolean hasCompletions() {
        return index.getSourcePartitionCount() > 0;
    }

    public synchronized long getPendingTransactionCount() {
        return pending.size();
    }

    public synchronized long getReservedOffsetRangeCount() {
        return reservedRanges;
    }

    public synchronized long getSourcePartitionCount() {
        return index.getSourcePartitionCount();
    }

    public synchronized long getOffsetRangeCount() {
        return index.getOffsetRangeCount();
    }

    public synchronized long getIdentityBytes() {
        return index.getIdentityBytes();
    }

    public void write(DataOutput output) throws IOException {
        index.write(output);
    }

    public void readFields(DataInput input) throws IOException {
        KafkaSourceCompletionIndex restored = KafkaSourceCompletionIndex.read(input, maxSourcePartitions,
                maxOffsetRanges, maxIdentityBytes);
        synchronized (this) {
            Preconditions.checkState(pending.isEmpty(), "Cannot load a Kafka source ledger with pending commits");
            for (KafkaSourceDescriptor descriptor : restored.snapshot()) {
                index.restore(descriptor);
            }
        }
    }

    public synchronized void resetForImageLoad() {
        Preconditions.checkState(pending.isEmpty(), "Cannot reset a Kafka source ledger with pending commits");
        index = new KafkaSourceCompletionIndex(maxSourcePartitions, maxOffsetRanges, maxIdentityBytes);
    }

    public static final class Reservation {
        private final KafkaSourceCompletionLedger owner;
        private final long transactionId;
        private final KafkaSourceDescriptor descriptor;
        private final ReservationCapacity capacity;
        private boolean published;
        private boolean released;

        private Reservation(KafkaSourceCompletionLedger owner, long transactionId, KafkaSourceDescriptor descriptor,
                ReservationCapacity capacity) {
            this.owner = owner;
            this.transactionId = transactionId;
            this.descriptor = descriptor;
            this.capacity = capacity;
        }
    }
}
