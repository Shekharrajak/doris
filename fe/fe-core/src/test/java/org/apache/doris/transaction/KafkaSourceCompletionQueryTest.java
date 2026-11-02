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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.load.routineload.RoutineLoadTaskInfo;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionLedger;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionLedger.Reservation;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.OffsetRange;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.Partition;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.thrift.TGetKafkaSourceCompletionsRequest;
import org.apache.doris.thrift.TGetKafkaSourceCompletionsResult;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TRLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class KafkaSourceCompletionQueryTest {
    private static final long DB_ID = 10;
    private static final long TABLE_ID = 20;
    private static final long JOB_ID = 30;
    private static final long TXN_ID = 40;
    private static final UUID TASK_ID = new UUID(50, 60);
    private static final String TOKEN = "query-token";

    private MockedStatic<Env> envStatic;
    private Env env;
    private InternalCatalog catalog;
    private Database database;
    private GlobalTransactionMgr transactions;
    private RoutineLoadManager jobs;
    private KafkaRoutineLoadJob job;
    private List<RoutineLoadTaskInfo> tasks;
    private TransactionState transaction;
    private KafkaSourceCompletionLedger ledger;
    private FrontendServiceImpl service;
    private String originalDeployMode;
    private String originalCloudUniqueId;

    private OffsetRange range(long first, long last) {
        return new OffsetRange(first, last);
    }

    private KafkaSourceDescriptor source(OffsetRange... ranges) {
        return new KafkaSourceDescriptor(JOB_ID, "source-cluster", Collections.singletonList(
                new Partition(7, 9, 0, Arrays.asList(ranges))));
    }

    private TransactionState transaction(LoadJobSourceType sourceType) {
        return new TransactionState(DB_ID, new ArrayList<>(Collections.singletonList(TABLE_ID)), TXN_ID,
                DebugUtil.printId(TASK_ID), new TUniqueId(50, 60), sourceType,
                new TxnCoordinator(TxnSourceType.BE, 1, "127.0.0.1", 0), JOB_ID, 10000);
    }

    @BeforeEach
    public void setUp() throws Exception {
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
        Config.deploy_mode = "local";
        Config.cloud_unique_id = "";
        env = Mockito.mock(Env.class);
        catalog = Mockito.mock(InternalCatalog.class);
        database = Mockito.mock(Database.class);
        transactions = Mockito.mock(GlobalTransactionMgr.class);
        jobs = Mockito.mock(RoutineLoadManager.class);
        ledger = Mockito.spy(new KafkaSourceCompletionLedger(100, 100, 10000, 10));
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getGlobalTransactionMgr()).thenReturn(transactions);
        Mockito.when(env.getRoutineLoadManager()).thenReturn(jobs);
        Mockito.when(env.getKafkaSourceCompletionLedger()).thenReturn(ledger);
        Mockito.when(catalog.getDbOrMetaException(DB_ID)).thenReturn(database);
        Mockito.when(database.getTableOrMetaException(TABLE_ID)).thenReturn(Mockito.mock(OlapTable.class));

        transaction = transaction(LoadJobSourceType.ROUTINE_LOAD_TASK);
        Mockito.when(transactions.getTransactionState(DB_ID, TXN_ID)).thenAnswer(invocation -> transaction);
        job = new KafkaRoutineLoadJob(JOB_ID, "share-query", DB_ID, TABLE_ID, "localhost:9092", "topic",
                UserIdentity.ADMIN);
        Deencapsulation.setField(job, "useShareConsumer", true);
        Deencapsulation.setField(job, "state", JobState.RUNNING);
        KafkaTaskInfo task = new KafkaTaskInfo(TASK_ID, JOB_ID, 10000, Collections.emptyMap(), false, 0, false);
        Deencapsulation.setField(task, "txnId", TXN_ID);
        task.setExecuteStartTimeMs(System.currentTimeMillis());
        tasks = new ArrayList<>(Collections.singletonList(task));
        Deencapsulation.setField(job, "routineLoadTaskInfoList", tasks);
        Mockito.when(jobs.getJob(JOB_ID)).thenReturn(job);
        service = Mockito.spy(new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class)));
        Mockito.doReturn(false).when(service).checkToken(Mockito.anyString());
        Mockito.doReturn(true).when(service).checkToken(TOKEN);
        ledger.replayCommitted(source(range(0, 0), range(2, 4), range(8, 9)));
        Mockito.clearInvocations(ledger);
    }

    @AfterEach
    public void tearDown() {
        if (envStatic != null) {
            envStatic.close();
        }
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
    }

    private TGetKafkaSourceCompletionsRequest request() {
        return new TGetKafkaSourceCompletionsRequest().setDbId(DB_ID).setTxnId(TXN_ID).setJobId(JOB_ID)
                .setTaskId(new TUniqueId(TASK_ID.getMostSignificantBits(), TASK_ID.getLeastSignificantBits()))
                .setSource(source(range(1, 3), range(6, 8)).toThrift()).setToken(TOKEN);
    }

    private void assertDenied(TGetKafkaSourceCompletionsResult result) {
        Assertions.assertNotNull(result.getStatus());
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, result.getStatus().getStatusCode());
        Assertions.assertFalse(result.isSetCompletedSource());
    }

    @Test
    public void testAuthenticatedQueryReturnsOnlyExactIntersectionAndIndependentCopies() throws Exception {
        TGetKafkaSourceCompletionsRequest request = request();
        TGetKafkaSourceCompletionsResult result = service.getKafkaSourceCompletions(request);
        KafkaSourceDescriptor expected = source(range(2, 3), range(8, 8));
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(expected.toThrift(), result.getCompletedSource());

        result.getCompletedSource().getPartitions().get(0).getTopicId().setHi(99);
        result.getCompletedSource().getPartitions().get(0).getRanges().get(0).setLast(100);
        TGetKafkaSourceCompletionsResult repeated = service.getKafkaSourceCompletions(request);
        Assertions.assertEquals(expected.toThrift(), repeated.getCompletedSource());
        Assertions.assertEquals(Collections.singletonList(source(range(0, 0), range(2, 4), range(8, 9))),
                ledger.snapshot());
    }

    @Test
    public void testServicePreservesPartitionTopicIncarnationAndClusterScopes() throws Exception {
        ledger.replayCommitted(new KafkaSourceDescriptor(JOB_ID, "source-cluster", Arrays.asList(
                new Partition(7, 9, 1, Arrays.asList(range(1, 2), range(5, 6))),
                new Partition(11, 13, 0, Arrays.asList(range(2, 3), range(8, 8))))));
        ledger.replayCommitted(new KafkaSourceDescriptor(JOB_ID, "source-cluster", Collections.singletonList(
                new Partition(7, 8, 0, Collections.singletonList(range(6, 6))))));
        ledger.replayCommitted(new KafkaSourceDescriptor(JOB_ID, "other-cluster", Collections.singletonList(
                new Partition(7, 9, 0, Collections.singletonList(range(6, 6))))));
        ledger.replayCommitted(new KafkaSourceDescriptor(JOB_ID + 1, "source-cluster", Collections.singletonList(
                new Partition(7, 9, 0, Collections.singletonList(range(7, 7))))));
        KafkaSourceDescriptor query = new KafkaSourceDescriptor(JOB_ID, "source-cluster", Arrays.asList(
                new Partition(7, 9, 0, Arrays.asList(range(1, 3), range(6, 8))),
                new Partition(7, 9, 1, Arrays.asList(range(0, 1), range(3, 5))),
                new Partition(11, 13, 0, Arrays.asList(range(1, 2), range(7, 9))),
                new Partition(11, 14, 0, Collections.singletonList(range(0, 9)))));
        KafkaSourceDescriptor expected = new KafkaSourceDescriptor(JOB_ID, "source-cluster", Arrays.asList(
                new Partition(7, 9, 0, Arrays.asList(range(2, 3), range(8, 8))),
                new Partition(7, 9, 1, Arrays.asList(range(1, 1), range(5, 5))),
                new Partition(11, 13, 0, Arrays.asList(range(2, 2), range(8, 8)))));

        TGetKafkaSourceCompletionsResult result = service.getKafkaSourceCompletions(
                request().setSource(query.toThrift()));
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(expected.toThrift(), result.getCompletedSource());
        KafkaSourceDescriptor otherCluster = new KafkaSourceDescriptor(JOB_ID, "other-cluster",
                Collections.singletonList(new Partition(7, 9, 0, Collections.singletonList(range(0, 9)))));
        TGetKafkaSourceCompletionsResult other = service.getKafkaSourceCompletions(
                request().setSource(otherCluster.toThrift()));
        Assertions.assertEquals(TStatusCode.OK, other.getStatus().getStatusCode());
        KafkaSourceDescriptor otherExpected = new KafkaSourceDescriptor(JOB_ID, "other-cluster",
                Collections.singletonList(new Partition(7, 9, 0, Collections.singletonList(range(6, 6)))));
        Assertions.assertEquals(otherExpected.toThrift(), other.getCompletedSource());
    }

    @Test
    public void testNoRetainedEvidenceReturnsOkWithoutCompletedSource() throws Exception {
        TGetKafkaSourceCompletionsRequest request = request().setSource(source(range(5, 7)).toThrift());
        TGetKafkaSourceCompletionsResult result = service.getKafkaSourceCompletions(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertFalse(result.isSetCompletedSource());
        Assertions.assertNull(transaction.getTxnCommitAttachment());
        Assertions.assertEquals(0, ledger.getPendingTransactionCount());
    }

    @Test
    public void testAuthenticationAndMasterFailuresExposeNoFacts() throws Exception {
        TGetKafkaSourceCompletionsResult invalid = service.getKafkaSourceCompletions(request().setToken("bad-token"));
        assertDenied(invalid);
        Assertions.assertFalse(String.valueOf(invalid.getStatus().getErrorMsgs()).contains("bad-token"));
        TGetKafkaSourceCompletionsRequest missing = request();
        missing.unsetToken();
        assertDenied(service.getKafkaSourceCompletions(missing));
        Mockito.when(env.isMaster()).thenReturn(false);
        TGetKafkaSourceCompletionsResult follower = service.getKafkaSourceCompletions(request());
        Assertions.assertEquals(TStatusCode.NOT_MASTER, follower.getStatus().getStatusCode());
        Assertions.assertFalse(follower.isSetCompletedSource());
        Mockito.verifyNoInteractions(ledger);
    }

    @ParameterizedTest
    @ValueSource(strings = {"database", "transaction", "job", "task", "task-part", "source", "range"})
    public void testMissingOrMalformedRequestIsRejected(String field) throws Exception {
        TGetKafkaSourceCompletionsRequest request = request();
        switch (field) {
            case "database":
                request.unsetDbId();
                break;
            case "transaction":
                request.unsetTxnId();
                break;
            case "job":
                request.unsetJobId();
                break;
            case "task":
                request.unsetTaskId();
                break;
            case "task-part":
                request.getTaskId().unsetLo();
                break;
            case "source":
                request.unsetSource();
                break;
            case "range":
                request.getSource().getPartitions().get(0).getRanges().get(0).unsetFirst();
                break;
            default:
                throw new AssertionError(field);
        }
        assertDenied(service.getKafkaSourceCompletions(request));
        Mockito.verifyNoInteractions(ledger);
    }

    @ParameterizedTest
    @ValueSource(strings = {"database", "transaction", "job", "task", "namespace", "stale"})
    public void testQueryMustOwnTheCurrentTaskAndNamespace(String mismatch) throws Exception {
        TGetKafkaSourceCompletionsRequest request = request();
        switch (mismatch) {
            case "database":
                request.setDbId(DB_ID + 1);
                Mockito.when(catalog.getDbOrMetaException(DB_ID + 1)).thenThrow(
                        new MetaNotFoundException("query database no longer exists"));
                break;
            case "transaction":
                request.setTxnId(TXN_ID + 1);
                break;
            case "job":
                request.setJobId(JOB_ID + 1);
                break;
            case "task":
                request.setTaskId(new TUniqueId(51, 61));
                break;
            case "namespace":
                request.getSource().setNamespaceId(JOB_ID + 1);
                break;
            case "stale":
                tasks.clear();
                break;
            default:
                throw new AssertionError(mismatch);
        }
        assertDenied(service.getKafkaSourceCompletions(request));
        Assertions.assertNull(transaction.getTxnCommitAttachment());
        Mockito.verifyNoInteractions(ledger);
    }

    @ParameterizedTest
    @ValueSource(strings = {"classic", "paused", "multi-table", "source", "callback", "table", "missing-table",
            "subtransactions", "status", "cloud"})
    public void testIneligibleJobOrTransactionIsRejected(String mismatch) throws Exception {
        switch (mismatch) {
            case "classic":
                Deencapsulation.setField(job, "useShareConsumer", false);
                break;
            case "paused":
                Deencapsulation.setField(job, "state", JobState.PAUSED);
                break;
            case "multi-table":
                job.setMultiTable(true);
                break;
            case "source":
                transaction = transaction(LoadJobSourceType.FRONTEND);
                break;
            case "callback":
                transaction.setCallbackId(JOB_ID + 1);
                break;
            case "table":
                transaction.getTableIdList().set(0, TABLE_ID + 1);
                break;
            case "missing-table":
                Mockito.when(database.getTableOrMetaException(TABLE_ID)).thenThrow(
                        new MetaNotFoundException("query table no longer exists"));
                break;
            case "subtransactions":
                transaction.setSubTxnIds(Collections.emptyList());
                break;
            case "status":
                transaction.setTransactionStatus(TransactionStatus.COMMITTED);
                break;
            case "cloud":
                Config.deploy_mode = "cloud";
                Config.cloud_unique_id = "query-cloud";
                break;
            default:
                throw new AssertionError(mismatch);
        }
        assertDenied(service.getKafkaSourceCompletions(request()));
        Assertions.assertNull(transaction.getTxnCommitAttachment());
        Mockito.verifyNoInteractions(ledger);
    }

    @Test
    public void testQueryDoesNotBindSourceOrPreventALaterSmallerCommitSubset() throws Exception {
        String before = GsonUtils.GSON.toJson(transaction);
        List<KafkaSourceDescriptor> completed = ledger.snapshot();
        TGetKafkaSourceCompletionsResult result = service.getKafkaSourceCompletions(request());
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(before, GsonUtils.GSON.toJson(transaction));
        Assertions.assertEquals(completed, ledger.snapshot());
        Assertions.assertEquals(0, ledger.getPendingTransactionCount());
        Assertions.assertEquals(0, ledger.getReservedOffsetRangeCount());
        Assertions.assertFalse(transaction.isKafkaSourceCompletionTracked());

        KafkaSourceDescriptor subset = source(range(6, 7));
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment(new TRLTaskTxnCommitAttachment()
                .setLoadSourceType(TLoadSourceType.KAFKA).setJobId(JOB_ID).setId(new TUniqueId(50, 60))
                .setKafkaRLTaskProgress(new TKafkaRLTaskProgress(Collections.emptyMap()))
                .setKafkaSourceDescriptor(subset.toThrift()));
        transaction.bindKafkaSourceCommitAttachment(attachment);
        Assertions.assertSame(attachment, transaction.getTxnCommitAttachment());
        Assertions.assertEquals(subset, transaction.getKafkaSourceDescriptor());
        Assertions.assertEquals(TransactionStatus.PREPARE, transaction.getTransactionStatus());
    }

    @Test
    public void testPendingReservationsAreNotReturnedAsCompleted() throws Exception {
        KafkaSourceDescriptor pending = source(range(6, 6));
        Reservation reservation = ledger.reserve(TXN_ID + 1, pending);
        try {
            TGetKafkaSourceCompletionsResult result = service.getKafkaSourceCompletions(
                    request().setSource(pending.toThrift()));
            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
            Assertions.assertFalse(result.isSetCompletedSource());
            Assertions.assertEquals(1, ledger.getPendingTransactionCount());
            Assertions.assertNull(transaction.getTxnCommitAttachment());
        } finally {
            ledger.releaseUncommitted(reservation);
        }
    }
}
