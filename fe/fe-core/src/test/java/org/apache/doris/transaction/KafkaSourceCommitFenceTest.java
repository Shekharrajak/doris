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

package org.apache.doris.transaction;

import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TKafkaSourceDescriptor;
import org.apache.doris.thrift.TKafkaSourceOffsetRange;
import org.apache.doris.thrift.TKafkaSourcePartition;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TRLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaSourceCommitFenceTest {
    private static final long JOB_ID = 100;

    private TransactionState transaction(LoadJobSourceType sourceType, long callbackId) {
        return new TransactionState(1, Collections.singletonList(2L), 3, "source_commit",
                new TUniqueId(4, 5), sourceType, new TxnCoordinator(TxnSourceType.BE, 0, "127.0.0.1", 0),
                callbackId, 1000);
    }

    private TransactionState transaction() {
        return transaction(LoadJobSourceType.ROUTINE_LOAD_TASK, JOB_ID);
    }

    private TKafkaSourceDescriptor descriptor(long jobId) {
        return new TKafkaSourceDescriptor().setNamespaceId(jobId).setClusterId("cluster-a")
                .setPartitions(Arrays.asList(new TKafkaSourcePartition().setTopicId(new TUniqueId(7, 9))
                                .setPartition(0).setRanges(Arrays.asList(
                                        new TKafkaSourceOffsetRange().setFirst(0).setLast(0),
                                        new TKafkaSourceOffsetRange().setFirst(2).setLast(4))),
                        new TKafkaSourcePartition().setTopicId(new TUniqueId(7, 9)).setPartition(1)
                                .setRanges(Collections.singletonList(
                                        new TKafkaSourceOffsetRange().setFirst(1).setLast(1)))));
    }

    private RLTaskTxnCommitAttachment attachment(TKafkaSourceDescriptor descriptor) {
        TRLTaskTxnCommitAttachment thrift = new TRLTaskTxnCommitAttachment().setJobId(JOB_ID)
                .setId(new TUniqueId(11, 12)).setLoadSourceType(TLoadSourceType.KAFKA)
                .setKafkaRLTaskProgress(new TKafkaRLTaskProgress(Collections.emptyMap())).setLoadedRows(6);
        if (descriptor != null) {
            thrift.setJobId(descriptor.getNamespaceId()).setKafkaSourceDescriptor(descriptor);
        }
        return new RLTaskTxnCommitAttachment(thrift);
    }

    private RLTaskTxnCommitAttachment attachment() {
        return attachment(descriptor(JOB_ID));
    }

    @ParameterizedTest
    @EnumSource(value = TransactionStatus.class, names = {"PREPARE", "COMMITTED", "VISIBLE"})
    public void testBoundAttachmentRequiresTheSameCompleteSourceSet(TransactionStatus status) throws Exception {
        TransactionState transaction = transaction();
        RLTaskTxnCommitAttachment original = attachment();
        transaction.bindKafkaSourceCommitAttachment(original);
        transaction.setTransactionStatus(status);

        transaction.bindKafkaSourceCommitAttachment(attachment());
        Assertions.assertSame(original, transaction.getTxnCommitAttachment());

        TKafkaSourceDescriptor cluster = descriptor(JOB_ID).setClusterId("cluster-b");
        TKafkaSourceDescriptor topic = descriptor(JOB_ID);
        topic.getPartitions().get(0).getTopicId().setLo(10);
        topic.getPartitions().get(1).getTopicId().setLo(10);
        TKafkaSourceDescriptor partition = descriptor(JOB_ID);
        partition.getPartitions().get(1).setPartition(2);
        TKafkaSourceDescriptor sparseSubset = descriptor(JOB_ID);
        sparseSubset.getPartitions().get(0).getRanges().get(1).setFirst(3);
        for (TKafkaSourceDescriptor changed : Arrays.asList(cluster, topic, partition, sparseSubset,
                descriptor(JOB_ID + 1))) {
            RLTaskTxnCommitAttachment incoming = attachment(changed);
            Assertions.assertThrows(TransactionCommitFailedException.class,
                    () -> transaction.bindKafkaSourceCommitAttachment(incoming));
            transaction.setTxnCommitAttachment(incoming);
            Assertions.assertSame(original, transaction.getTxnCommitAttachment());
        }
        for (TxnCommitAttachment removed : Arrays.asList(null, attachment(null))) {
            Assertions.assertThrows(TransactionCommitFailedException.class,
                    () -> transaction.bindKafkaSourceCommitAttachment(removed));
            transaction.setTxnCommitAttachment(removed);
            Assertions.assertSame(original, transaction.getTxnCommitAttachment());
        }
    }

    @Test
    public void testBindingRequiresRoutineLoadSourceAndMatchingCallback() {
        for (LoadJobSourceType sourceType : LoadJobSourceType.values()) {
            if (sourceType != LoadJobSourceType.ROUTINE_LOAD_TASK) {
                TransactionState transaction = transaction(sourceType, JOB_ID);
                Assertions.assertThrows(TransactionCommitFailedException.class,
                        () -> transaction.bindKafkaSourceCommitAttachment(attachment()));
                Assertions.assertNull(transaction.getTxnCommitAttachment());
            }
        }
        TransactionState transaction = transaction(LoadJobSourceType.ROUTINE_LOAD_TASK, JOB_ID + 1);
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> transaction.bindKafkaSourceCommitAttachment(attachment()));
        Assertions.assertNull(transaction.getTxnCommitAttachment());

        TransactionState subTransaction = transaction();
        RLTaskTxnCommitAttachment legacy = attachment(null);
        subTransaction.setTxnCommitAttachment(legacy);
        subTransaction.setSubTxnIds(Collections.emptyList());
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> subTransaction.bindKafkaSourceCommitAttachment(attachment()));
        Assertions.assertSame(legacy, subTransaction.getTxnCommitAttachment());
    }

    @ParameterizedTest
    @EnumSource(value = TransactionStatus.class, names = {"COMMITTED", "VISIBLE", "ABORTED"})
    public void testTerminalTransactionCannotAcquireSourceIdentity(TransactionStatus status) {
        TransactionState transaction = transaction();
        RLTaskTxnCommitAttachment legacy = attachment(null);
        transaction.setTxnCommitAttachment(legacy);
        transaction.setTransactionStatus(status);
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> transaction.bindKafkaSourceCommitAttachment(attachment()));
        transaction.setTxnCommitAttachmentForAbort(attachment());
        Assertions.assertSame(legacy, transaction.getTxnCommitAttachment());
    }

    @ParameterizedTest
    @EnumSource(value = TransactionStatus.class, names = {"PREPARE", "COMMITTED", "VISIBLE", "ABORTED"})
    public void testAbortCannotRewriteBoundIdentity(TransactionStatus status) throws Exception {
        TransactionState transaction = transaction();
        RLTaskTxnCommitAttachment original = attachment();
        transaction.bindKafkaSourceCommitAttachment(original);
        transaction.setTransactionStatus(status);
        for (TxnCommitAttachment incoming : Arrays.asList(null, attachment(null), attachment(),
                attachment(descriptor(JOB_ID).setClusterId("cluster-b")))) {
            transaction.setTxnCommitAttachmentForAbort(incoming);
            Assertions.assertSame(original, transaction.getTxnCommitAttachment());
        }

        TransactionState legacy = transaction();
        RLTaskTxnCommitAttachment legacyAttachment = attachment(null);
        legacy.setTxnCommitAttachmentForAbort(legacyAttachment);
        Assertions.assertSame(legacyAttachment, legacy.getTxnCommitAttachment());
        Assertions.assertDoesNotThrow(legacy::awaitKafkaSourceCommitDurability);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDurabilityWaitReleasesMonitorAndPropagatesOutcome(boolean fail) throws Exception {
        TransactionState transaction = transaction();
        transaction.bindKafkaSourceCommitAttachment(attachment());
        transaction.beginKafkaSourceCommitDurability();
        transaction.setTransactionStatus(TransactionStatus.COMMITTED);
        Assertions.assertThrows(IllegalStateException.class, transaction::markReplayedKafkaSourceCommitDurable);

        CountDownLatch entered = new CountDownLatch(1);
        CompletableFuture<Void> waiter = CompletableFuture.runAsync(() -> {
            entered.countDown();
            try {
                transaction.awaitKafkaSourceCommitDurability();
            } catch (TransactionCommitFailedException e) {
                throw new CompletionException(e);
            }
        });
        IOException failure = new IOException("journal flush failed");
        try {
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class, () -> waiter.get(100, TimeUnit.MILLISECONDS));
        } finally {
            CompletableFuture.runAsync(() -> {
                if (fail) {
                    transaction.failKafkaSourceCommitDurability(failure);
                } else {
                    transaction.completeKafkaSourceCommitDurability();
                }
            }).get(5, TimeUnit.SECONDS);
        }

        if (fail) {
            ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                    () -> waiter.get(5, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(TransactionCommitFailedException.class, exception.getCause());
            Assertions.assertSame(failure, exception.getCause().getCause());
            transaction.completeKafkaSourceCommitDurability();
            Assertions.assertThrows(TransactionCommitFailedException.class,
                    transaction::awaitKafkaSourceCommitDurability);
            Assertions.assertThrows(IllegalStateException.class, transaction::markReplayedKafkaSourceCommitDurable);
            TransactionCommitFailedException unchanged = Assertions.assertThrows(TransactionCommitFailedException.class,
                    transaction::awaitKafkaSourceCommitDurability);
            Assertions.assertSame(failure, unchanged.getCause());
        } else {
            waiter.get(5, TimeUnit.SECONDS);
            Assertions.assertDoesNotThrow(transaction::awaitKafkaSourceCommitDurability);
        }
    }

    @ParameterizedTest
    @EnumSource(value = TransactionStatus.class,
            names = {"PREPARE", "PRECOMMITTED", "COMMITTED", "VISIBLE", "ABORTED"})
    public void testUncoveredStateCannotReportDurability(TransactionStatus status) throws Exception {
        TransactionState transaction = transaction();
        transaction.bindKafkaSourceCommitAttachment(attachment());
        transaction.setTransactionStatus(status);
        Assertions.assertThrows(TransactionCommitFailedException.class,
                transaction::awaitKafkaSourceCommitDurability);
    }

    @ParameterizedTest
    @EnumSource(value = TransactionStatus.class, names = {"COMMITTED", "VISIBLE"})
    public void testGsonRestoreNeedsReplayDurabilityMark(TransactionStatus status) throws Exception {
        TransactionState transaction = transaction();
        RLTaskTxnCommitAttachment original = attachment();
        transaction.bindKafkaSourceCommitAttachment(original);
        String beforeBegin = GsonUtils.GSON.toJson(transaction);
        transaction.beginKafkaSourceCommitDurability();
        Assertions.assertEquals(beforeBegin, GsonUtils.GSON.toJson(transaction));
        transaction.setTransactionStatus(status);
        transaction.completeKafkaSourceCommitDurability();

        TransactionState restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(transaction), TransactionState.class);
        Assertions.assertEquals(original.getKafkaSourceDescriptor(),
                ((RLTaskTxnCommitAttachment) restored.getTxnCommitAttachment()).getKafkaSourceDescriptor());
        Assertions.assertThrows(TransactionCommitFailedException.class, restored::awaitKafkaSourceCommitDurability);
        restored.markReplayedKafkaSourceCommitDurable();
        Assertions.assertDoesNotThrow(restored::awaitKafkaSourceCommitDurability);
        restored.markReplayedKafkaSourceCommitDurable();
        Assertions.assertDoesNotThrow(restored::awaitKafkaSourceCommitDurability);
    }
}
