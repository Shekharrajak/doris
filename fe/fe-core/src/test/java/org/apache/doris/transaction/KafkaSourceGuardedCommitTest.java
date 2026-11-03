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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.load.routineload.RoutineLoadTaskInfo;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionLedger;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionQuery;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.thrift.TCommitTxnRequest;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TKafkaSourceDescriptor;
import org.apache.doris.thrift.TKafkaSourceOffsetRange;
import org.apache.doris.thrift.TKafkaSourcePartition;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TRLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaSourceGuardedCommitTest {
    private static final long JOB_ID = 500;
    private static final long LEGACY_JOB_ID = 501;
    private static final TxnCoordinator COORDINATOR = new TxnCoordinator(TxnSourceType.BE, 0, "be1", 0);

    private FakeEditLog fakeEditLog;
    private FakeEnv fakeEnv;
    private FakeTransactionIDGenerator fakeIds;
    private Env env;
    private GlobalTransactionMgr transactions;
    private DatabaseTransactionMgr manager;
    private KafkaSourceCompletionLedger ledger;
    private final Map<Long, List<RoutineLoadTaskInfo>> tasks = new HashMap<>();
    private final Map<Long, UUID> taskIds = new HashMap<>();
    private boolean originalTracking;
    private boolean originalLogOutsideLock;
    private String originalDeployMode;
    private String originalCloudUniqueId;
    private long taskSequence;

    @BeforeEach
    public void setUp() throws Exception {
        originalTracking = Config.enable_kafka_source_completion_tracking;
        originalLogOutsideLock = Config.enable_txn_log_outside_lock;
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
        Config.enable_kafka_source_completion_tracking = true;
        Config.enable_txn_log_outside_lock = true;
        Config.deploy_mode = "local";
        Config.cloud_unique_id = "";
        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        fakeIds = new FakeTransactionIDGenerator();
        env = CatalogTestUtil.createTestCatalog();
        MetaContext context = new MetaContext();
        context.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        context.setThreadLocalInfo();
        transactions = (GlobalTransactionMgr) env.getGlobalTransactionMgr();
        transactions.setEditLog(env.getEditLog());
        manager = transactions.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        ledger = new KafkaSourceCompletionLedger(10, 100, 10000, 10);
        Deencapsulation.setField(env, "kafkaSourceCompletionLedger", ledger);
        registerJob(JOB_ID, 1);
        registerJob(LEGACY_JOB_ID, 0);
    }

    @AfterEach
    public void tearDown() {
        if (fakeIds != null) {
            fakeIds.close();
        }
        if (fakeEnv != null) {
            fakeEnv.close();
        }
        if (fakeEditLog != null) {
            fakeEditLog.close();
        }
        Config.enable_kafka_source_completion_tracking = originalTracking;
        Config.enable_txn_log_outside_lock = originalLogOutsideLock;
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
    }

    private void registerJob(long jobId, int guardVersion) {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(jobId, "share-" + jobId,
                CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, "localhost:9092", "topic", UserIdentity.ADMIN);
        Deencapsulation.setField(job, "useShareConsumer", true);
        Deencapsulation.setField(job, "kafkaSourceGuardVersion", guardVersion);
        Deencapsulation.setField(job, "state", JobState.RUNNING);
        List<RoutineLoadTaskInfo> jobTasks = new ArrayList<>();
        Deencapsulation.setField(job, "routineLoadTaskInfoList", jobTasks);
        tasks.put(jobId, jobTasks);
        env.getRoutineLoadManager().replayCreateRoutineLoadJob(job);
        TxnStateChangeCallback callback = Mockito.mock(TxnStateChangeCallback.class);
        Mockito.when(callback.getId()).thenReturn(jobId);
        transactions.getCallbackFactory().addCallback(callback);
    }

    private TransactionState begin(long jobId) throws Exception {
        UUID taskId = new UUID(1, ++taskSequence);
        long id = transactions.beginTransaction(CatalogTestUtil.testDbId1,
                Collections.singletonList(CatalogTestUtil.testTableId1), DebugUtil.printId(taskId), null,
                COORDINATOR, LoadJobSourceType.ROUTINE_LOAD_TASK, jobId, Config.stream_load_default_timeout_second);
        KafkaTaskInfo task = new KafkaTaskInfo(taskId, jobId, 10000, Collections.emptyMap(), false, 0, false);
        Deencapsulation.setField(task, "txnId", id);
        task.setExecuteStartTimeMs(System.currentTimeMillis());
        tasks.get(jobId).add(task);
        taskIds.put(id, taskId);
        return transactions.getTransactionState(CatalogTestUtil.testDbId1, id);
    }

    private TransactionState begin() throws Exception {
        return begin(JOB_ID);
    }

    private RLTaskTxnCommitAttachment attachment(TransactionState state, int proof, boolean withSource,
            long... offsets) {
        TRLTaskTxnCommitAttachment thrift = new TRLTaskTxnCommitAttachment().setLoadSourceType(TLoadSourceType.KAFKA)
                .setJobId(state.getCallbackId()).setId(new TUniqueId(3, 4))
                .setKafkaRLTaskProgress(new TKafkaRLTaskProgress(Collections.emptyMap()))
                .setLoadedRows(Math.max(1, offsets.length));
        if (proof != 0) {
            thrift.setKafkaSourceGuardVersion(proof);
        }
        if (withSource) {
            List<TKafkaSourceOffsetRange> ranges = new ArrayList<>();
            for (long offset : offsets) {
                TKafkaSourceOffsetRange previous = ranges.isEmpty() ? null : ranges.get(ranges.size() - 1);
                if (previous != null && offset > previous.getLast() && offset - previous.getLast() == 1) {
                    previous.setLast(offset);
                } else {
                    ranges.add(new TKafkaSourceOffsetRange().setFirst(offset).setLast(offset));
                }
            }
            thrift.setKafkaSourceDescriptor(new TKafkaSourceDescriptor().setNamespaceId(state.getCallbackId())
                    .setClusterId("source-cluster").setPartitions(Collections.singletonList(new TKafkaSourcePartition()
                            .setTopicId(new TUniqueId(1, 2)).setPartition(0).setRanges(ranges))));
        }
        return new RLTaskTxnCommitAttachment(thrift);
    }

    private RLTaskTxnCommitAttachment attachment(TransactionState state, long... offsets) {
        return attachment(state, 1, true, offsets);
    }

    private List<Table> tables() throws Exception {
        return Collections.singletonList(env.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1));
    }

    private void commit(TransactionState state, RLTaskTxnCommitAttachment attachment) throws Exception {
        manager.commitTransaction(tables(), state.getTransactionId(),
                GlobalTransactionMgrTest.generateTabletCommitInfos(CatalogTestUtil.testTabletId1,
                        GlobalTransactionMgrTest.allBackends), attachment, false);
    }

    private Optional<KafkaSourceDescriptor> query(TransactionState state, KafkaSourceDescriptor source)
            throws Exception {
        return KafkaSourceCompletionQuery.getCompleted(env, state.getDbId(), state.getTransactionId(),
                state.getCallbackId(), taskIds.get(state.getTransactionId()), source);
    }

    private void assertUncommitted(TransactionState state) {
        Assertions.assertEquals(TransactionStatus.PREPARE, state.getTransactionStatus());
        Assertions.assertTrue(state.getIdToTableCommitInfos().isEmpty());
        Assertions.assertFalse(state.isKafkaSourceCompletionTracked());
    }

    @Test
    public void testPersistedJobPolicyIsBoundBeforePrepareBecomesVisible() throws Exception {
        DatabaseTransactionMgr observed = Mockito.spy(manager);
        AtomicReference<TransactionState> inserted = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            TransactionState state = invocation.getArgument(0);
            Assertions.assertEquals(1, state.getKafkaSourceGuardVersionRequired());
            Assertions.assertNull(observed.getTransactionState(state.getTransactionId()));
            inserted.set(state);
            return invocation.callRealMethod();
        }).when(observed).unprotectUpdateInMemoryState(Mockito.any(TransactionState.class), Mockito.eq(false));
        long id = observed.beginTransaction(Collections.singletonList(CatalogTestUtil.testTableId1), "guarded-policy",
                null, COORDINATOR, LoadJobSourceType.ROUTINE_LOAD_TASK, JOB_ID,
                Config.stream_load_default_timeout_second);
        Assertions.assertSame(inserted.get(), observed.getTransactionState(id));
        Assertions.assertThrows(IllegalStateException.class,
                () -> inserted.get().requireKafkaSourceGuardVersion(0));

        TransactionState legacy = begin(LEGACY_JOB_ID);
        Assertions.assertEquals(0, legacy.getKafkaSourceGuardVersionRequired());
        JsonObject old = JsonParser.parseString(GsonUtils.GSON.toJson(inserted.get())).getAsJsonObject();
        Assertions.assertEquals(1, old.get("kafkaSourceGuardVersionRequired").getAsInt());
        old.remove("kafkaSourceGuardVersionRequired");
        Assertions.assertEquals(0, GsonUtils.GSON.fromJson(old, TransactionState.class)
                .getKafkaSourceGuardVersionRequired());
        Config.enable_kafka_source_completion_tracking = false;
        Assertions.assertThrows(UserException.class, this::begin);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGuardedCommitRequiresProofSourceAndTrackingBeforeTerminalRetry(boolean outsideLock)
            throws Exception {
        Config.enable_txn_log_outside_lock = outsideLock;
        TransactionState state = begin();
        List<RLTaskTxnCommitAttachment> invalid = new ArrayList<>();
        invalid.add(attachment(state, 0, true, 1));
        invalid.add(attachment(state, 2, true, 1));
        invalid.add(attachment(state, 1, false));
        invalid.add(null);
        for (String field : new String[] {"loadedRows", "filteredRows", "unselectedRows"}) {
            RLTaskTxnCommitAttachment badRows = attachment(state, 1);
            Deencapsulation.setField(badRows, field, field.equals("loadedRows") ? 0L : 1L);
            invalid.add(badRows);
        }
        for (RLTaskTxnCommitAttachment incoming : invalid) {
            Assertions.assertThrows(TransactionCommitFailedException.class, () -> commit(state, incoming));
            assertUncommitted(state);
            Assertions.assertNull(state.getTxnCommitAttachment());
        }
        Config.enable_kafka_source_completion_tracking = false;
        Assertions.assertThrows(TransactionCommitFailedException.class, () -> commit(state, attachment(state, 1)));
        assertUncommitted(state);
        Config.enable_kafka_source_completion_tracking = true;
        RLTaskTxnCommitAttachment accepted = attachment(state, 1);
        commit(state, accepted);
        Assertions.assertEquals(TransactionStatus.COMMITTED, state.getTransactionStatus());
        Assertions.assertEquals(Optional.of(accepted.getKafkaSourceDescriptor()),
                ledger.getCompleted(accepted.getKafkaSourceDescriptor()));
        for (RLTaskTxnCommitAttachment incoming : invalid) {
            Assertions.assertThrows(TransactionCommitFailedException.class, () -> commit(state, incoming));
            Assertions.assertSame(accepted, state.getTxnCommitAttachment());
        }
        Config.enable_kafka_source_completion_tracking = false;
        Assertions.assertThrows(TransactionCommitFailedException.class, () -> commit(state, attachment(state, 1)));
        Assertions.assertSame(accepted, state.getTxnCommitAttachment());
    }

    @Test
    public void testGuardedTransactionsCannotUseDescriptorlessAlternateCommitRoutes() throws Exception {
        TransactionState state = begin();
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> manager.preCommitTransaction2PC(tables(), state.getTransactionId(),
                        Collections.emptyList(), null));
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> manager.commitTransaction(tables(), state.getTransactionId(), null, null, true));
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> manager.commitTransaction(state.getTransactionId(), tables(), Collections.emptyList()));
        FrontendServiceImpl service = Mockito.spy(new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class)));
        Mockito.doReturn(true).when(service).checkToken("guarded-token");
        TCommitTxnRequest request = new TCommitTxnRequest().setUser("root").setPasswd("")
                .setDb(CatalogTestUtil.testDb1).setDbId(state.getDbId()).setTxnId(state.getTransactionId())
                .setToken("guarded-token").setTxnInsert(true).setSubTxnInfos(Collections.emptyList());
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, service.commitTxn(request).getStatus().getStatusCode());
        Assertions.assertNull(state.getSubTxnIds());
        Assertions.assertNull(state.getTxnCommitAttachment());
        assertUncommitted(state);
        Assertions.assertEquals(0, ledger.getPendingTransactionCount());
    }

    @Test
    public void testNativeCommitClaimsRejectOverlapUntilJournalPublishesDone() throws Exception {
        TransactionState first = begin();
        TransactionState overlapping = begin();
        TransactionState independent = begin();
        RLTaskTxnCommitAttachment firstSource = attachment(first, 1, 2);
        RLTaskTxnCommitAttachment overlapSource = attachment(overlapping, 2, 3);
        CountDownLatch journalEntered = new CountDownLatch(1);
        CountDownLatch releaseJournal = new CountDownLatch(1);
        EditLog.EditLogItem item = Mockito.mock(EditLog.EditLogItem.class);
        Mockito.doAnswer(invocation -> {
            journalEntered.countDown();
            Assertions.assertTrue(releaseJournal.await(10, TimeUnit.SECONDS));
            return 1L;
        }).when(item).await();
        Mockito.when(env.getEditLog().submitEdit(Mockito.eq(OperationType.OP_UPSERT_TRANSACTION_STATE),
                Mockito.same(first))).thenReturn(item);
        FutureTask<Void> owner = new FutureTask<>(() -> {
            try (FakeEnv threadEnv = new FakeEnv()) {
                commit(first, firstSource);
            }
            return null;
        });
        Thread ownerThread = new Thread(owner, "guarded-source-journal");
        ownerThread.start();
        try {
            Assertions.assertTrue(journalEntered.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(TransactionStatus.COMMITTED, first.getTransactionStatus());
            Assertions.assertEquals(1, ledger.getPendingTransactionCount());
            Assertions.assertEquals(1, ledger.getPendingClaimRangeCount());
            Assertions.assertTrue(query(overlapping, overlapSource.getKafkaSourceDescriptor()).isEmpty());
            TransactionCommitFailedException rejected = Assertions.assertThrows(TransactionCommitFailedException.class,
                    () -> commit(overlapping, overlapSource));
            Assertions.assertTrue(rejected.getMessage().contains("PAUSE"), rejected.getMessage());
            assertUncommitted(overlapping);
            Assertions.assertEquals(1, ledger.getPendingTransactionCount());
            commit(independent, attachment(independent, 3));
            Assertions.assertEquals(TransactionStatus.COMMITTED, independent.getTransactionStatus());
            Assertions.assertEquals(Optional.of(attachment(overlapping, 3).getKafkaSourceDescriptor()),
                    query(overlapping, overlapSource.getKafkaSourceDescriptor()));
            releaseJournal.countDown();
            owner.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(0, ledger.getPendingTransactionCount());
            Assertions.assertEquals(0, ledger.getPendingClaimRangeCount());
            Assertions.assertEquals(Optional.of(overlapSource.getKafkaSourceDescriptor()),
                    query(overlapping, overlapSource.getKafkaSourceDescriptor()));
            Assertions.assertThrows(TransactionCommitFailedException.class, () -> commit(overlapping, overlapSource));
            assertUncommitted(overlapping);
        } finally {
            releaseJournal.countDown();
            ownerThread.join(10000);
            Assertions.assertFalse(ownerThread.isAlive());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testUnknownJournalOutcomeKeepsClaimsUntilMatchingReplay(boolean enqueueFailure) throws Exception {
        TransactionState first = begin();
        TransactionState overlapping = begin();
        RLTaskTxnCommitAttachment firstSource = attachment(first, 1, 2);
        RLTaskTxnCommitAttachment overlapSource = attachment(overlapping, 2, 3);
        AtomicReference<String> recorded = new AtomicReference<>();
        EditLog.EditLogItem item = Mockito.mock(EditLog.EditLogItem.class);
        Mockito.when(item.await()).thenThrow(new IllegalStateException("journal outcome unknown"));
        Mockito.doAnswer(invocation -> {
            recorded.set(GsonUtils.GSON.toJson(first));
            if (enqueueFailure) {
                throw new IllegalStateException("journal enqueue outcome unknown");
            }
            return item;
        }).when(env.getEditLog()).submitEdit(Mockito.eq(OperationType.OP_UPSERT_TRANSACTION_STATE),
                Mockito.same(first));
        Assertions.assertThrows(IllegalStateException.class, () -> commit(first, firstSource));
        Assertions.assertEquals(TransactionStatus.COMMITTED, first.getTransactionStatus());
        Assertions.assertEquals(1, ledger.getPendingTransactionCount());
        Assertions.assertEquals(1, ledger.getPendingClaimRangeCount());
        Assertions.assertTrue(query(overlapping, overlapSource.getKafkaSourceDescriptor()).isEmpty());
        Assertions.assertThrows(TransactionCommitFailedException.class, () -> commit(overlapping, overlapSource));
        Assertions.assertThrows(TransactionCommitFailedException.class, () -> commit(first, firstSource));
        Assertions.assertThrows(IllegalStateException.class, () -> manager.replayUpsertTransactionState(first));
        Assertions.assertEquals(1, ledger.getPendingTransactionCount());
        TransactionState replay = GsonUtils.GSON.fromJson(recorded.get(), TransactionState.class);
        Assertions.assertEquals(1, replay.getKafkaSourceGuardVersionRequired());
        manager.replayUpsertTransactionState(replay);
        Assertions.assertEquals(0, ledger.getPendingTransactionCount());
        Assertions.assertEquals(0, ledger.getPendingClaimRangeCount());
        Assertions.assertDoesNotThrow(replay::awaitKafkaSourceCommitDurability);
        Assertions.assertEquals(Optional.of(attachment(overlapping, 2).getKafkaSourceDescriptor()),
                query(overlapping, overlapSource.getKafkaSourceDescriptor()));
        Assertions.assertThrows(TransactionCommitFailedException.class, () -> commit(overlapping, overlapSource));
        assertUncommitted(overlapping);
    }

    @Test
    public void testGuardedReplayRejectsMissingDurableEvidenceAndRetainsRequiredPolicy() throws Exception {
        TransactionState state = begin();
        RLTaskTxnCommitAttachment source = attachment(state, 8);
        commit(state, source);
        JsonObject committed = JsonParser.parseString(GsonUtils.GSON.toJson(state)).getAsJsonObject();
        KafkaSourceCompletionLedger restoredLedger = new KafkaSourceCompletionLedger(0, 0, 0, 0);
        Deencapsulation.setField(env, "kafkaSourceCompletionLedger", restoredLedger);
        for (String field : new String[] {"kafkaSourceCompletionTracked", "kafkaSourceDescriptor",
                "kafkaSourceGuardVersion", "kafkaSourceGuardVersionRequired"}) {
            JsonObject invalid = committed.deepCopy();
            if (field.equals("kafkaSourceCompletionTracked")) {
                invalid.addProperty(field, false);
            } else if (field.equals("kafkaSourceGuardVersionRequired")) {
                invalid.addProperty(field, 2);
            } else {
                invalid.getAsJsonObject("txnCommitAttachment").remove(field);
            }
            TransactionState malformed = GsonUtils.GSON.fromJson(invalid, TransactionState.class);
            Assertions.assertThrows(IllegalStateException.class, () -> manager.replayUpsertTransactionState(malformed));
            Assertions.assertTrue(restoredLedger.snapshot().isEmpty());
        }
        Config.enable_kafka_source_completion_tracking = false;
        TransactionState replay = GsonUtils.GSON.fromJson(committed, TransactionState.class);
        manager.replayUpsertTransactionState(replay);
        Assertions.assertEquals(1, replay.getKafkaSourceGuardVersionRequired());
        Assertions.assertEquals(Optional.of(source.getKafkaSourceDescriptor()),
                restoredLedger.getCompleted(source.getKafkaSourceDescriptor()));
        Assertions.assertDoesNotThrow(replay::awaitKafkaSourceCommitDurability);
    }

    @Test
    public void testLegacyTransactionsKeepAtLeastOnceCommitBehavior() throws Exception {
        Config.enable_kafka_source_completion_tracking = false;
        TransactionState first = begin(LEGACY_JOB_ID);
        TransactionState second = begin(LEGACY_JOB_ID);
        Assertions.assertEquals(0, first.getKafkaSourceGuardVersionRequired());
        commit(first, attachment(first, 0, true, 7));
        commit(second, attachment(second, 0, true, 7));
        Assertions.assertEquals(TransactionStatus.COMMITTED, first.getTransactionStatus());
        Assertions.assertEquals(TransactionStatus.COMMITTED, second.getTransactionStatus());
        Assertions.assertTrue(ledger.snapshot().isEmpty());
        Assertions.assertEquals(0, ledger.getPendingTransactionCount());
    }
}
