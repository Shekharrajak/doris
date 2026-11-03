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
    private final Map<Long, NamespaceReservations> namespaceReservations = new HashMap<>();
    private final KafkaSourcePendingClaims pendingClaims;
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
        this.pendingClaims = new KafkaSourcePendingClaims(maxSourcePartitions, maxOffsetRanges, maxIdentityBytes);
    }

    public synchronized Reservation reserve(long transactionId, KafkaSourceDescriptor descriptor)
            throws CapacityExceededException {
        try {
            return reserve(transactionId, descriptor, false);
        } catch (SourceConflictException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public synchronized Reservation reserveExclusive(long transactionId, KafkaSourceDescriptor descriptor)
            throws CapacityExceededException, SourceConflictException {
        return reserve(transactionId, descriptor, true);
    }

    private Reservation reserve(long transactionId, KafkaSourceDescriptor descriptor, boolean exclusive)
            throws CapacityExceededException, SourceConflictException {
        Preconditions.checkArgument(transactionId > 0, "Kafka source transaction ID must be positive");
        Reservation existing = pending.get(transactionId);
        if (existing != null) {
            Preconditions.checkState(existing.descriptor.equals(descriptor),
                    "Kafka source reservation identity changed for transaction %s", transactionId);
            Preconditions.checkState(existing.exclusive == exclusive,
                    "Kafka source reservation mode changed for transaction %s", transactionId);
            return existing;
        }
        NamespaceReservations namespace = namespaceReservations.get(descriptor.getNamespaceId());
        if (namespace != null && namespace.exclusive != exclusive) {
            throw new SourceConflictException("Kafka source reservation mode conflicts for namespace "
                    + descriptor.getNamespaceId());
        }
        if (exclusive && index.overlaps(descriptor)) {
            throw new SourceConflictException("Kafka source records are already completed for transaction "
                    + transactionId);
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
        Reservation reservation = new Reservation(this, transactionId, descriptor, capacity, exclusive);
        if (exclusive) {
            pendingClaims.reserve(transactionId, descriptor);
        }
        pending.put(transactionId, reservation);
        namespaceReservations.put(descriptor.getNamespaceId(),
                new NamespaceReservations(exclusive, namespace == null ? 1 : namespace.count + 1));
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
        NamespaceReservations namespace = namespaceReservations.get(reservation.descriptor.getNamespaceId());
        Preconditions.checkState(namespace != null && namespace.exclusive == reservation.exclusive,
                "Kafka source namespace reservation is missing or has a different mode");
        if (reservation.exclusive) {
            pendingClaims.remove(reservation.transactionId, reservation.descriptor);
        }
        pending.remove(reservation.transactionId);
        if (namespace.count == 1) {
            namespaceReservations.remove(reservation.descriptor.getNamespaceId());
        } else {
            namespaceReservations.put(reservation.descriptor.getNamespaceId(),
                    new NamespaceReservations(namespace.exclusive, namespace.count - 1));
        }
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

    public synchronized long getPendingClaimPartitionCount() {
        return pendingClaims.getSourcePartitionCount();
    }

    public synchronized long getPendingClaimRangeCount() {
        return pendingClaims.getOffsetRangeCount();
    }

    public synchronized long getPendingClaimIdentityBytes() {
        return pendingClaims.getIdentityBytes();
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

    private record NamespaceReservations(boolean exclusive, int count) {
    }

    public static final class SourceConflictException extends Exception {
        public SourceConflictException(String message) {
            super(message);
        }
    }

    public static final class Reservation {
        private final KafkaSourceCompletionLedger owner;
        private final long transactionId;
        private final KafkaSourceDescriptor descriptor;
        private final ReservationCapacity capacity;
        private final boolean exclusive;
        private boolean published;
        private boolean released;

        private Reservation(KafkaSourceCompletionLedger owner, long transactionId, KafkaSourceDescriptor descriptor,
                ReservationCapacity capacity, boolean exclusive) {
            this.owner = owner;
            this.transactionId = transactionId;
            this.descriptor = descriptor;
            this.capacity = capacity;
            this.exclusive = exclusive;
        }
    }
}
