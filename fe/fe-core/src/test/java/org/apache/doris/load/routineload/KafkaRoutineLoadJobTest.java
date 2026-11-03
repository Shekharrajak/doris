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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.kafka.KafkaUtil;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSeparator;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class KafkaRoutineLoadJobTest {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJobTest.class);
    private static final String GUARDED_CREATE_SQL = "CREATE ROUTINE LOAD guarded ON events "
            + "PROPERTIES(\"strict_mode\"=\"true\",\"max_error_number\"=\"0\",\"max_filter_ratio\"=\"0\") "
            + "FROM KAFKA(\"kafka_broker_list\"=\"localhost:9092\",\"kafka_topic\"=\"events\","
            + "\"kafka_consumer_type\"=\"share\",\"kafka_share_guarded_commit\"=\"true\")";

    private String jobName = "job1";
    private String dbName = "db1";
    private LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, jobName);
    private String tableNameString = "table1";
    private String topicName = "topic1";
    private String serverAddress = "http://127.0.0.1:8080";
    private String kafkaPartitionString = "1,2,3";

    private PartitionNamesInfo partitionNames;

    private Separator columnSeparator = new Separator(",");

    private String sequenceColumnName = "source_sequence";

    ConnectContext connectContext = Mockito.mock(ConnectContext.class);
    TResourceInfo tResourceInfo = Mockito.mock(TResourceInfo.class);

    private MockedStatic<ConnectContext> connectContextStatic;

    @BeforeEach
    public void init() {
        connectContextStatic = MockedAuth.mockedConnectContext(connectContext, "root", "192.168.1.1");

        List<String> partitionNameList = Lists.newArrayList();
        partitionNameList.add("p1");
        partitionNames = new PartitionNamesInfo(false, partitionNameList);
    }

    @AfterEach
    public void tearDown() {
        if (connectContextStatic != null) {
            connectContextStatic.close();
        }
    }

    @Test
    public void testShareConsumerConcurrencyAndSerialization() throws Exception {
        int originalLimit = Config.max_routine_load_task_concurrent_num;
        try {
            Config.max_routine_load_task_concurrent_num = 9;
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "share", 1L,
                    1L, "127.0.0.1:9092", "topic", UserIdentity.ADMIN);
            Deencapsulation.setField(job, "currentKafkaPartitions", Arrays.asList(0, 1, 2));
            Deencapsulation.setField(job, "desireTaskConcurrentNum", 9);
            Assertions.assertEquals(3, job.calculateCurrentConcurrentTaskNum());
            Deencapsulation.setField(job, "useShareConsumer", true);
            Assertions.assertEquals(9, job.calculateCurrentConcurrentTaskNum());
            Assertions.assertTrue(job.hasMoreDataToConsume(UUID.randomUUID(), Maps.newHashMap()));
            String json = org.apache.doris.persist.gson.GsonUtils.GSON.toJson(job);
            Assertions.assertTrue(com.google.gson.JsonParser.parseString(json).getAsJsonObject()
                    .get("useShareConsumer").getAsBoolean());
        } finally {
            Config.max_routine_load_task_concurrent_num = originalLimit;
        }
    }

    @Test
    public void testGuardedCreationRequiresTrackingAndZeroLossQuality() throws Exception {
        boolean previousTracking = Config.enable_kafka_source_completion_tracking;
        String previousMode = Config.deploy_mode;
        String previousCloud = Config.cloud_unique_id;
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(Mockito.mock(Env.class));
            Config.deploy_mode = "local";
            Config.cloud_unique_id = "";
            Config.enable_kafka_source_completion_tracking = false;
            Assertions.assertThrows(UserException.class,
                    () -> new GuardedCreationJob().configure(guardedCreateInfo()));
            Config.enable_kafka_source_completion_tracking = true;
            GuardedCreationJob job = new GuardedCreationJob();
            job.configure(guardedCreateInfo());
            Assertions.assertEquals(1, job.getKafkaSourceGuardVersion());
            Assertions.assertEquals("true", job.getDataSourceProperties()
                    .get(KafkaDataSourceProperties.KAFKA_SHARE_GUARDED_COMMIT));

            for (int invalid = 0; invalid < 5; ++invalid) {
                CreateRoutineLoadInfo info = guardedCreateInfo();
                if (invalid == 0) {
                    Mockito.when(info.isStrictMode()).thenReturn(false);
                } else if (invalid == 1) {
                    Mockito.when(info.getMaxErrorNum()).thenReturn(1L);
                } else if (invalid == 2) {
                    Mockito.when(info.getMaxFilterRatio()).thenReturn(1.0);
                } else {
                    RoutineLoadDesc desc = Mockito.mock(RoutineLoadDesc.class);
                    if (invalid == 3) {
                        Mockito.when(desc.getFilter()).thenReturn(Mockito.mock(Expr.class));
                    } else {
                        Mockito.when(desc.getPrecedingFilter())
                                .thenReturn(Mockito.mock(Expr.class));
                    }
                    Mockito.when(info.getRoutineLoadDesc()).thenReturn(desc);
                }
                Assertions.assertThrows(UserException.class, () -> new GuardedCreationJob().configure(info));
            }
            Config.cloud_unique_id = "cloud";
            Assertions.assertThrows(UserException.class,
                    () -> new GuardedCreationJob().configure(guardedCreateInfo()));
        } finally {
            Config.enable_kafka_source_completion_tracking = previousTracking;
            Config.deploy_mode = previousMode;
            Config.cloud_unique_id = previousCloud;
        }
    }

    @Test
    public void testGuardedModeReplayRemainsExplicitAndIgnoresCurrentTrackingConfig() throws Exception {
        boolean previous = Config.enable_kafka_source_completion_tracking;
        String previousMode = Config.deploy_mode;
        String previousCloud = Config.cloud_unique_id;
        Env env = replayEnv();
        CreateRoutineLoadInfo info = guardedCreateInfo();
        CreateRoutineLoadCommand command = Mockito.mock(CreateRoutineLoadCommand.class);
        Mockito.when(command.getCreateRoutineLoadInfo()).thenReturn(info);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedConstruction<NereidsParser> ignored = Mockito.mockConstruction(NereidsParser.class,
                        (parser, context) -> Mockito.when(parser.parseSingle(Mockito.anyString())).thenReturn(command))) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Config.deploy_mode = "local";
            Config.cloud_unique_id = "";
            Config.enable_kafka_source_completion_tracking = true;
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "guarded", 1L, 1L,
                    "localhost:9092", "events", UserIdentity.ADMIN);
            job.setOrigStmt(new OriginStatement(GUARDED_CREATE_SQL, 0));
            Deencapsulation.invoke(job, "setOptional", info);
            JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(job)).getAsJsonObject();
            Assertions.assertEquals("KafkaRoutineLoadJob", json.get("clazz").getAsString());
            Config.enable_kafka_source_completion_tracking = false;
            KafkaRoutineLoadJob restored = GsonUtils.GSON.fromJson(json.toString(), KafkaRoutineLoadJob.class);
            Assertions.assertEquals(1, restored.getKafkaSourceGuardVersion());
            Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, restored.getState());
            Assertions.assertEquals(0.0, (Double) Deencapsulation.getField(restored, "maxFilterRatio"));
            json.remove("kafkaSourceGuardVersion");
            KafkaRoutineLoadJob missingMode = GsonUtils.GSON.fromJson(json.toString(), KafkaRoutineLoadJob.class);
            Assertions.assertEquals(0, missingMode.getKafkaSourceGuardVersion());
            Assertions.assertEquals(RoutineLoadJob.JobState.CANCELLED, missingMode.getState());
            json.addProperty("kafkaSourceGuardVersion", 2);
            Assertions.assertThrows(JsonParseException.class,
                    () -> GsonUtils.GSON.fromJson(json.toString(), KafkaRoutineLoadJob.class));
            json.addProperty("kafkaSourceGuardVersion", 1);
            json.addProperty("useShareConsumer", false);
            Assertions.assertThrows(JsonParseException.class,
                    () -> GsonUtils.GSON.fromJson(json.toString(), KafkaRoutineLoadJob.class));
        } finally {
            Config.enable_kafka_source_completion_tracking = previous;
            Config.deploy_mode = previousMode;
            Config.cloud_unique_id = previousCloud;
        }
    }

    @Test
    public void testGuardedAlterCannotRelaxQualityOrChangeSource() throws Exception {
        GuardedCreationJob job = new GuardedCreationJob();
        Deencapsulation.setField(job, "useShareConsumer", true);
        Deencapsulation.setField(job, "kafkaSourceGuardVersion", 1);
        for (Map<String, String> properties : Arrays.asList(
                Map.of(CreateRoutineLoadInfo.STRICT_MODE, "false"),
                Map.of(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY, "1"),
                Map.of(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.1"))) {
            AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
            Mockito.when(command.getAnalyzedJobProperties()).thenReturn(properties);
            Assertions.assertThrows(UserException.class, () -> job.modifyProperties(command));
        }
        AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
        Mockito.when(command.getAnalyzedJobProperties()).thenReturn(Map.of());
        Mockito.when(command.getDataSourceProperties()).thenReturn(new KafkaDataSourceProperties(
                Map.of(KafkaDataSourceProperties.KAFKA_SHARE_GUARDED_COMMIT, "false")));
        Assertions.assertThrows(UserException.class, () -> job.modifyProperties(command));
        for (boolean preceding : new boolean[] {false, true}) {
            RoutineLoadDesc desc = Mockito.mock(RoutineLoadDesc.class);
            if (preceding) {
                Mockito.when(desc.getPrecedingFilter()).thenReturn(Mockito.mock(Expr.class));
            } else {
                Mockito.when(desc.getFilter()).thenReturn(Mockito.mock(Expr.class));
            }
            AlterRoutineLoadCommand filter = Mockito.mock(AlterRoutineLoadCommand.class);
            Mockito.when(filter.getAnalyzedJobProperties()).thenReturn(Map.of());
            Mockito.when(filter.getRoutineLoadDesc()).thenReturn(desc);
            Assertions.assertThrows(UserException.class, () -> job.modifyProperties(filter));
        }
    }

    @Test
    public void testReplayedCreateCannotPromoteMissingPersistedMode() throws Exception {
        Env env = replayEnv();
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            for (boolean guarded : new boolean[] {false, true}) {
                CreateRoutineLoadInfo info = guardedCreateInfo();
                if (!guarded) {
                    KafkaDataSourceProperties legacy = new KafkaDataSourceProperties(Map.of(
                            "kafka_broker_list", "localhost:9092", "kafka_topic", "events"));
                    legacy.convertAndCheckDataSourceProperties();
                    Mockito.when(info.getDataSourceProperties()).thenReturn(legacy);
                }
                CreateRoutineLoadCommand command = Mockito.mock(CreateRoutineLoadCommand.class);
                Mockito.when(command.getCreateRoutineLoadInfo()).thenReturn(info);
                try (MockedConstruction<NereidsParser> ignored = Mockito.mockConstruction(NereidsParser.class,
                        (parser, context) -> Mockito.when(parser.parseSingle(Mockito.anyString()))
                                .thenReturn(command))) {
                    GuardedCreationJob job = new GuardedCreationJob();
                    Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.RUNNING);
                    job.gsonPostProcess();
                    Assertions.assertEquals(0, job.getKafkaSourceGuardVersion());
                    Assertions.assertEquals(guarded ? RoutineLoadJob.JobState.CANCELLED
                            : RoutineLoadJob.JobState.RUNNING, job.getState());
                    Mockito.verify(info).validate(Mockito.any(ConnectContext.class));
                    Mockito.verify(info).getDataSourceProperties();
                }
            }
        }
    }

    @Test
    public void testGuardedPauseRequiresManualResume() {
        GuardedCreationJob job = new GuardedCreationJob();
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(job, "pauseReason",
                new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR, "guarded submit failed"));
        Deencapsulation.setField(job, "kafkaSourceGuardVersion", 1);
        boolean guardedResume = Deencapsulation.invoke(job, "needAutoResume");
        Assertions.assertFalse(guardedResume);
        Assertions.assertEquals(0L, (Long) Deencapsulation.getField(job, "latestResumeTimestamp"));
        Deencapsulation.setField(job, "kafkaSourceGuardVersion", 0);
        boolean legacyResume = Deencapsulation.invoke(job, "needAutoResume");
        Assertions.assertTrue(legacyResume);
    }

    @Test
    public void testGuardedAbortPauseMarkerTakesPriorityOverNestedTimeout() throws Exception {
        String reason = "[INTERNAL_ERROR] PAUSE: Guarded Kafka share task failed: [TIMEOUT] load timed out";
        Assertions.assertEquals(TransactionState.TxnStatusChangeReason.TIMEOUT,
                TransactionState.TxnStatusChangeReason.fromString(reason));
        Env env = Mockito.mock(Env.class);
        RoutineLoadTaskScheduler scheduler = Mockito.mock(RoutineLoadTaskScheduler.class);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(org.apache.doris.persist.EditLog.class));
        Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(scheduler);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            GuardedCreationJob job = new GuardedCreationJob();
            Deencapsulation.setField(job, "useShareConsumer", true);
            Deencapsulation.setField(job, "kafkaSourceGuardVersion", 1);
            Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.RUNNING);
            KafkaTaskInfo task = new KafkaTaskInfo(UUID.randomUUID(), 1L, 20000,
                    Maps.newHashMap(), false, -1, false);
            Deencapsulation.setField(task, "txnId", 7L);
            Deencapsulation.setField(job, "routineLoadTaskInfoList", Lists.newArrayList(task));
            TransactionState transaction = new TransactionState();
            Deencapsulation.setField(transaction, "transactionId", 7L);
            job.beforeAborted(transaction);
            transaction.setTransactionStatus(TransactionStatus.ABORTED);
            job.afterAborted(transaction, true, reason);
            Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, job.getState());
            Assertions.assertEquals(InternalErrorCode.CANNOT_RESUME_ERR, job.getPauseReason().getCode());
            List<RoutineLoadTaskInfo> remaining = Deencapsulation.getField(job, "routineLoadTaskInfoList");
            Assertions.assertTrue(remaining.isEmpty());
            Mockito.verifyNoInteractions(scheduler);
        }
    }

    private CreateRoutineLoadInfo guardedCreateInfo() throws UserException {
        Map<String, String> source = new HashMap<>();
        source.put("kafka_broker_list", "localhost:9092");
        source.put("kafka_topic", "events");
        source.put(KafkaDataSourceProperties.KAFKA_CONSUMER_TYPE, "share");
        source.put(KafkaDataSourceProperties.KAFKA_SHARE_GUARDED_COMMIT, "true");
        KafkaDataSourceProperties properties = new KafkaDataSourceProperties(source);
        properties.convertAndCheckDataSourceProperties();
        CreateRoutineLoadInfo info = Mockito.mock(CreateRoutineLoadInfo.class);
        Mockito.when(info.getDataSourceProperties()).thenReturn(properties);
        Mockito.when(info.getFileFormatProperties()).thenReturn(new CsvFileFormatProperties("csv"));
        Mockito.when(info.getUniqueKeyUpdateMode()).thenReturn(TUniqueKeyUpdateMode.UPSERT);
        Mockito.when(info.getTimezone()).thenReturn("UTC");
        Mockito.when(info.isStrictMode()).thenReturn(true);
        return info;
    }

    private Env replayEnv() {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Database database = Mockito.mock(Database.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getDb(1L)).thenReturn(Optional.of(database));
        Mockito.when(catalog.getDb("db1")).thenReturn(Optional.of(database));
        Mockito.when(database.getName()).thenReturn("db1");
        Mockito.when(database.getId()).thenReturn(1L);
        return env;
    }

    private static class GuardedCreationJob extends KafkaRoutineLoadJob {
        GuardedCreationJob() {
            super(1L, "guarded", 1L, 1L, "localhost:9092", "events", UserIdentity.ADMIN);
            setOrigStmt(new OriginStatement(GUARDED_CREATE_SQL, 0));
        }

        void configure(CreateRoutineLoadInfo info) throws UserException {
            setOptional(info);
        }

    }

    @Test
    public void testRoutineLoadTaskConcurrentNum() throws MetaNotFoundException {
        Config.max_routine_load_task_concurrent_num = 6;

        List<Integer> partitionList1 = Lists.newArrayList(1, 2);
        List<Integer> partitionList2 = Lists.newArrayList(1, 2, 3);
        List<Integer> partitionList3 = Lists.newArrayList(1, 2, 3, 4);
        List<Integer> partitionList4 = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);

        // 2 partitions, 1 be
        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                        1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList1);
        Assertions.assertEquals(2, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 3 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList2);
        Assertions.assertEquals(3, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 4 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList3);
        Assertions.assertEquals(4, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 7 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList4);
        Assertions.assertEquals(6, routineLoadJob.calculateCurrentConcurrentTaskNum());
    }

    @Test
    public void testDivideRoutineLoadJob() throws UserException {
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        Env env = Mockito.mock(Env.class);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = Mockito.mock(RoutineLoadTaskScheduler.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(routineLoadTaskScheduler);

            RoutineLoadJob routineLoadJob =
                    new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                            1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);

            Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", Arrays.asList(1, 4, 6));

            routineLoadJob.divideRoutineLoadJob(2);

            // todo(ml): assert
            List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
            Assertions.assertEquals(2, routineLoadTaskInfoList.size());
            for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadTaskInfoList) {
                KafkaTaskInfo kafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
                Assertions.assertEquals(false, kafkaTaskInfo.isRunning());
                if (kafkaTaskInfo.getPartitions().size() == 2) {
                    Assertions.assertTrue(kafkaTaskInfo.getPartitions().contains(1));
                    Assertions.assertTrue(kafkaTaskInfo.getPartitions().contains(6));
                } else if (kafkaTaskInfo.getPartitions().size() == 1) {
                    Assertions.assertTrue(kafkaTaskInfo.getPartitions().contains(4));
                } else {
                    Assertions.fail();
                }
            }
        }
    }

    @Test
    public void testShareTasksHaveNoPartitionOwnership() throws Exception {
        Env env = Mockito.mock(Env.class);
        RoutineLoadTaskScheduler scheduler = Mockito.mock(RoutineLoadTaskScheduler.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(scheduler);
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "share", 1L,
                    1L, "localhost:9092", "topic", UserIdentity.ADMIN);
            Deencapsulation.setField(job, "useShareConsumer", true);
            job.divideRoutineLoadJob(9);
            List<RoutineLoadTaskInfo> tasks = Deencapsulation.getField(job, "routineLoadTaskInfoList");
            Assertions.assertEquals(9, tasks.size());
            for (RoutineLoadTaskInfo task : tasks) {
                Assertions.assertTrue(((KafkaTaskInfo) task).getPartitions().isEmpty());
            }
            Mockito.verify(scheduler).addTasksInQueue(tasks);
        }
    }

    @Test
    public void testUpdateLagRefreshesLatestOffsetCache() throws UserException {
        String originalCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "test-cloud";
            KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                    1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
            routineLoadJob.setCloudCluster("routine-load-compute-group");
            Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
            partitionIdToOffset.put(1, 10L);
            partitionIdToOffset.put(2, 20L);
            Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));

            try (MockedStatic<KafkaUtil> kafkaUtilStatic = Mockito.mockStatic(KafkaUtil.class)) {
                kafkaUtilStatic.when(() -> KafkaUtil.getLatestOffsets(Mockito.eq(1L), Mockito.any(UUID.class),
                        Mockito.eq("127.0.0.1:9020"), Mockito.eq("topic1"), Mockito.anyMap(), Mockito.anyList(),
                        Mockito.eq("routine-load-compute-group")))
                        .thenReturn(Lists.newArrayList(Pair.of(1, 15L), Pair.of(2, 30L)));

                routineLoadJob.updateLag();

                Assertions.assertEquals(15L, routineLoadJob.totalLag().longValue());
            }
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testUpdateLagRebuildsConvertedPropertiesAfterReplay() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "customKafkaPartitions", Lists.newArrayList(1));

        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("security.protocol", "SASL_PLAINTEXT");
        customProperties.put("sasl.mechanism", "PLAIN");
        Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);
        Deencapsulation.setField(routineLoadJob, "convertedCustomProperties", Maps.newHashMap());

        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 10L);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));

        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<KafkaUtil> kafkaUtilStatic = Mockito.mockStatic(KafkaUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            kafkaUtilStatic.when(() -> KafkaUtil.getLatestOffsets(Mockito.eq(1L), Mockito.any(UUID.class),
                    Mockito.eq("127.0.0.1:9020"), Mockito.eq("topic1"),
                    Mockito.<Map<String, String>>argThat(properties ->
                            "SASL_PLAINTEXT".equals(properties.get("security.protocol"))
                                    && "PLAIN".equals(properties.get("sasl.mechanism"))),
                    Mockito.argThat(partitions -> partitions.size() == 1 && partitions.contains(1)),
                    Mockito.nullable(String.class)))
                    .thenReturn(Lists.newArrayList(Pair.of(1, 15L)));

            routineLoadJob.updateLag();

            Assertions.assertEquals(5L, routineLoadJob.totalLag().longValue());
            kafkaUtilStatic.verify(() -> KafkaUtil.getLatestOffsets(Mockito.eq(1L), Mockito.any(UUID.class),
                    Mockito.eq("127.0.0.1:9020"), Mockito.eq("topic1"),
                    Mockito.<Map<String, String>>argThat(properties ->
                            "SASL_PLAINTEXT".equals(properties.get("security.protocol"))
                                    && "PLAIN".equals(properties.get("sasl.mechanism"))),
                    Mockito.argThat(partitions -> partitions.size() == 1 && partitions.contains(1)),
                    Mockito.nullable(String.class)));
        }
    }

    @Test
    public void testUpdateProgressWarnsWhenReadCommittedTaskHasZeroRowsAndLag() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("isolation.level", "read_committed");
        Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);

        Map<Integer, Long> cachedPartitionWithLatestOffsets = Maps.newHashMap();
        cachedPartitionWithLatestOffsets.put(1, 20L);
        Deencapsulation.setField(routineLoadJob, "cachedPartitionWithLatestOffsets",
                cachedPartitionWithLatestOffsets);

        Map<Integer, Long> taskProgress = Maps.newHashMap();
        taskProgress.put(1, 10L);
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        Deencapsulation.setField(attachment, "progress", new KafkaProgress(taskProgress));

        Deencapsulation.invoke(routineLoadJob, "updateProgress", attachment);

        String otherMsg = Deencapsulation.getField(routineLoadJob, "otherMsg");
        Assertions.assertTrue(otherMsg.contains("some records may be in uncommitted transactions"));
    }

    @Test
    public void testDisplayCustomPropertiesMasksKafkaSecrets() {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("security.protocol", "SASL_PLAINTEXT");
        customProperties.put("sasl.username", "doris");
        customProperties.put("sasl.password", "plain_secret");
        customProperties.put("sasl.jaas.config", "username=\"doris\" password=\"jaas_secret\"");
        customProperties.put("sasl.oauthbearer.client.secret", "oauth_client_secret");
        customProperties.put("sasl.oauthbearer.client.credentials.client.secret", "oauth_alias_secret");
        customProperties.put("sasl.oauthbearer.assertion.private.key.pem", "oauth_private_key_pem");
        customProperties.put("sasl.oauthbearer.assertion.private.key.passphrase", "oauth_private_key_passphrase");
        customProperties.put("ssl.keystore.password", "keystore_secret");
        customProperties.put("ssl.keystore.key", "keystore_key_secret");
        customProperties.put("ssl.key.pem", "key_pem_secret");
        customProperties.put(KafkaConfiguration.AWS_ACCESS_KEY, "aws_access_key");
        customProperties.put("aws.secret_key", "aws_secret");
        customProperties.put("aws.session_key", "aws_session_secret");
        customProperties.put("password", "bare_password_secret");
        customProperties.put("secret_key", "bare_secret_key");
        customProperties.put("session_token", "bare_session_token");
        Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);

        String customPropertiesJson = routineLoadJob.customPropertiesJsonToString();
        Map<String, String> showCreateCustomProperties = routineLoadJob.getCustomProperties();

        Assertions.assertFalse(customPropertiesJson.contains("plain_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("jaas_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("oauth_client_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("oauth_alias_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("oauth_private_key_pem"));
        Assertions.assertFalse(customPropertiesJson.contains("oauth_private_key_passphrase"));
        Assertions.assertFalse(customPropertiesJson.contains("keystore_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("keystore_key_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("key_pem_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("aws_access_key"));
        Assertions.assertFalse(customPropertiesJson.contains("aws_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("aws_session_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("bare_password_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("bare_secret_key"));
        Assertions.assertFalse(customPropertiesJson.contains("bare_session_token"));
        Assertions.assertTrue(customPropertiesJson.contains("\"sasl.password\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"sasl.jaas.config\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"sasl.oauthbearer.client.secret\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains(
                "\"sasl.oauthbearer.client.credentials.client.secret\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"sasl.oauthbearer.assertion.private.key.pem\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains(
                "\"sasl.oauthbearer.assertion.private.key.passphrase\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"ssl.keystore.password\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"ssl.keystore.key\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"ssl.key.pem\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"aws.access_key\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"aws.secret_key\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"aws.session_key\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"password\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"secret_key\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"session_token\":\"******\""));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.sasl.password"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.sasl.jaas.config"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.sasl.oauthbearer.client.secret"));
        Assertions.assertEquals("******",
                showCreateCustomProperties.get("property.sasl.oauthbearer.client.credentials.client.secret"));
        Assertions.assertEquals("******",
                showCreateCustomProperties.get("property.sasl.oauthbearer.assertion.private.key.pem"));
        Assertions.assertEquals("******",
                showCreateCustomProperties.get("property.sasl.oauthbearer.assertion.private.key.passphrase"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.ssl.keystore.password"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.ssl.keystore.key"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.ssl.key.pem"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.aws.access_key"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.aws.secret_key"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.aws.session_key"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.password"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.secret_key"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.session_token"));
        Assertions.assertEquals("doris", showCreateCustomProperties.get("property.sasl.username"));
        Assertions.assertEquals("plain_secret", customProperties.get("sasl.password"));
    }

    @Test
    public void testShareBatchSchedulingAndRenewal() throws UserException {
        RoutineLoadManager manager = Mockito.mock(RoutineLoadManager.class);
        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(manager);
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "share_job", 1L,
                    1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
            Mockito.when(manager.getJob(1L)).thenReturn(job);
            KafkaTaskInfo task = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                    Maps.newHashMap(), false, 1000, false);
            for (boolean share : new boolean[] {false, true}) {
                Deencapsulation.setField(job, "useShareConsumer", share);
                for (String field : new String[] {"loadedRows", "filteredRows", "unselectedRows", "receivedBytes"}) {
                    RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
                    Deencapsulation.setField(attachment, field, 1L);
                    task.handleTaskByTxnCommitAttachment(attachment);
                    Assertions.assertEquals(!share, task.getIsEof(), field);
                    Assertions.assertEquals(!share, task.needDedalySchedule(), field);
                    KafkaTaskInfo renewed = new KafkaTaskInfo(task, Maps.newHashMap(), false);
                    Assertions.assertEquals(!share, renewed.needDedalySchedule(), field);
                }
            }
            Deencapsulation.setField(job, "useShareConsumer", true);
            for (long elapsed : new long[] {1L, job.getMaxBatchIntervalS() * 1000}) {
                RLTaskTxnCommitAttachment empty = new RLTaskTxnCommitAttachment();
                Deencapsulation.setField(empty, "taskExecutionTimeMs", elapsed);
                task.handleTaskByTxnCommitAttachment(empty);
                Assertions.assertTrue(task.needDedalySchedule());
                Assertions.assertTrue(new KafkaTaskInfo(task, Maps.newHashMap(), false).needDedalySchedule());
            }
        }
    }

    @Test
    public void testReadCommittedZeroRowsWithLagDelaysNextTask() throws UserException {
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        Env env = Mockito.mock(Env.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);

            KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                    1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
            Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

            Map<String, String> customProperties = Maps.newHashMap();
            customProperties.put("isolation.level", "read_committed");
            Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);

            Map<Integer, Long> cachedPartitionWithLatestOffsets = Maps.newHashMap();
            cachedPartitionWithLatestOffsets.put(1, 20L);
            Deencapsulation.setField(routineLoadJob, "cachedPartitionWithLatestOffsets",
                    cachedPartitionWithLatestOffsets);

            Map<Integer, Long> taskProgress = Maps.newHashMap();
            taskProgress.put(1, 10L);
            Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(taskProgress));

            KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                    taskProgress, false, 1000, false);
            List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
            routineLoadTaskInfoList.add(kafkaTaskInfo);
            Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

            RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
            Deencapsulation.setField(attachment, "progress", new KafkaProgress(taskProgress));
            Deencapsulation.setField(attachment, "taskExecutionTimeMs",
                    routineLoadJob.getMaxBatchIntervalS() * 1000);

            kafkaTaskInfo.handleTaskByTxnCommitAttachment(attachment);

            Assertions.assertFalse(kafkaTaskInfo.getIsEof());
            Assertions.assertTrue(kafkaTaskInfo.needDedalySchedule());

            RoutineLoadTaskInfo newTask = Deencapsulation.invoke(routineLoadJob,
                    "unprotectRenewTask", kafkaTaskInfo, false);
            Assertions.assertTrue(newTask.needDedalySchedule());
        }
    }

    @Test
    public void testAdaptiveBatchUsesTaskLagThreshold() {
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        Env env = Mockito.mock(Env.class);
        int previousAdaptiveIntervalSec = Config.routine_load_adaptive_min_batch_interval_sec;

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Config.routine_load_adaptive_min_batch_interval_sec = 360;
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);

            KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                    1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
            Deencapsulation.setField(routineLoadJob, "maxBatchIntervalS", 20L);
            Deencapsulation.setField(routineLoadJob, "maxBatchRows", 200000L);
            Deencapsulation.setField(routineLoadJob, "maxBatchSizeBytes", 100L * 1024 * 1024);
            Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

            Map<Integer, Long> taskProgress = Maps.newHashMap();
            taskProgress.put(1, 10L);
            taskProgress.put(2, 20L);

            KafkaTaskInfo taskWithUnknownLag = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                    taskProgress, false, 1000, false);
            taskWithUnknownLag.updateAdaptiveTimeout(routineLoadJob);
            TRoutineLoadTask unknownLagThriftTask = new TRoutineLoadTask();
            Deencapsulation.invoke(
                    taskWithUnknownLag, "adaptiveBatchParam", unknownLagThriftTask, routineLoadJob);
            Assertions.assertEquals(20L, unknownLagThriftTask.getMaxIntervalS());

            Map<Integer, Long> latestOffsets = Maps.newHashMap();
            latestOffsets.put(1, 10_000_010L);
            latestOffsets.put(2, 10_000_020L);
            latestOffsets.put(3, 100_000_000L);
            Deencapsulation.setField(routineLoadJob, "cachedPartitionWithLatestOffsets", latestOffsets);

            KafkaTaskInfo taskAtLagThreshold = new KafkaTaskInfo(new UUID(1, 2), 1L, 20000,
                    taskProgress, false, 1000, false);
            taskAtLagThreshold.updateAdaptiveTimeout(routineLoadJob);
            latestOffsets.put(2, 10_000_021L);
            TRoutineLoadTask thresholdThriftTask = new TRoutineLoadTask();
            Deencapsulation.invoke(
                    taskAtLagThreshold, "adaptiveBatchParam", thresholdThriftTask, routineLoadJob);
            Assertions.assertEquals(20L, thresholdThriftTask.getMaxIntervalS());
            Assertions.assertEquals(200000L, thresholdThriftTask.getMaxBatchRows());
            Assertions.assertEquals(100L * 1024 * 1024, thresholdThriftTask.getMaxBatchSize());
            Assertions.assertEquals(routineLoadJob.getTimeout() * 1000L, taskAtLagThreshold.getTimeoutMs());

            KafkaTaskInfo taskAboveLagThreshold = new KafkaTaskInfo(new UUID(1, 3), 1L, 20000,
                    taskProgress, false, 1000, true);
            taskAboveLagThreshold.updateAdaptiveTimeout(routineLoadJob);
            TRoutineLoadTask adaptiveThriftTask = new TRoutineLoadTask();
            Deencapsulation.invoke(
                    taskAboveLagThreshold, "adaptiveBatchParam", adaptiveThriftTask, routineLoadJob);
            Assertions.assertEquals(360L, adaptiveThriftTask.getMaxIntervalS());
            Assertions.assertEquals(RoutineLoadJob.DEFAULT_MAX_BATCH_ROWS,
                    adaptiveThriftTask.getMaxBatchRows());
            Assertions.assertEquals(RoutineLoadJob.DEFAULT_MAX_BATCH_SIZE,
                    adaptiveThriftTask.getMaxBatchSize());
            Assertions.assertEquals(360L * Config.routine_load_task_timeout_multiplier * 1000,
                    taskAboveLagThreshold.getTimeoutMs());

            Deencapsulation.setField(routineLoadJob, "maxBatchRows", 50_000_000L);
            latestOffsets.put(1, 25_000_010L);
            latestOffsets.put(2, 25_000_020L);
            KafkaTaskInfo taskAtConfiguredLagThreshold = new KafkaTaskInfo(new UUID(1, 4), 1L, 20000,
                    taskProgress, false, 1000, false);
            taskAtConfiguredLagThreshold.updateAdaptiveTimeout(routineLoadJob);
            latestOffsets.put(2, 25_000_021L);
            TRoutineLoadTask configuredThresholdThriftTask = new TRoutineLoadTask();
            Deencapsulation.invoke(taskAtConfiguredLagThreshold, "adaptiveBatchParam",
                    configuredThresholdThriftTask, routineLoadJob);
            Assertions.assertEquals(20L, configuredThresholdThriftTask.getMaxIntervalS());
            Assertions.assertEquals(50_000_000L, configuredThresholdThriftTask.getMaxBatchRows());
            Assertions.assertEquals(routineLoadJob.getTimeout() * 1000L,
                    taskAtConfiguredLagThreshold.getTimeoutMs());

            KafkaTaskInfo taskAboveConfiguredLagThreshold = new KafkaTaskInfo(new UUID(1, 5), 1L, 20000,
                    taskProgress, false, 1000, false);
            taskAboveConfiguredLagThreshold.updateAdaptiveTimeout(routineLoadJob);
            TRoutineLoadTask configuredAdaptiveThriftTask = new TRoutineLoadTask();
            Deencapsulation.invoke(taskAboveConfiguredLagThreshold, "adaptiveBatchParam",
                    configuredAdaptiveThriftTask, routineLoadJob);
            Assertions.assertEquals(360L, configuredAdaptiveThriftTask.getMaxIntervalS());
            Assertions.assertEquals(50_000_000L, configuredAdaptiveThriftTask.getMaxBatchRows());
        } finally {
            Config.routine_load_adaptive_min_batch_interval_sec = previousAdaptiveIntervalSec;
        }
    }

    @Test
    public void testAdaptiveBatchUsesCapturedIntervalAcrossConfigChange() {
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        Env env = Mockito.mock(Env.class);
        int previousAdaptiveIntervalSec = Config.routine_load_adaptive_min_batch_interval_sec;

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Config.routine_load_adaptive_min_batch_interval_sec = 360;
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);

            KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                    1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
            Deencapsulation.setField(routineLoadJob, "maxBatchIntervalS", 30L);
            Deencapsulation.setField(routineLoadJob, "maxBatchRows", 200000L);
            Deencapsulation.setField(routineLoadJob, "maxBatchSizeBytes", 100L * 1024 * 1024);
            Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

            Map<Integer, Long> taskProgress = Maps.newHashMap();
            taskProgress.put(1, 10L);
            Map<Integer, Long> latestOffsets = Maps.newHashMap();
            latestOffsets.put(1, 20_000_011L);
            Deencapsulation.setField(routineLoadJob, "cachedPartitionWithLatestOffsets", latestOffsets);

            KafkaTaskInfo scheduledTask = new KafkaTaskInfo(new UUID(1, 6), 1L, 20000,
                    taskProgress, false, 1000, false);
            scheduledTask.updateAdaptiveTimeout(routineLoadJob);
            long adaptiveTimeoutMs = 360L * Config.routine_load_task_timeout_multiplier * 1000L;
            Assertions.assertEquals(adaptiveTimeoutMs, scheduledTask.getTimeoutMs());

            Config.routine_load_adaptive_min_batch_interval_sec = 720;
            TRoutineLoadTask scheduledThriftTask = new TRoutineLoadTask();
            Deencapsulation.invoke(scheduledTask, "adaptiveBatchParam", scheduledThriftTask, routineLoadJob);
            Assertions.assertEquals(360L, scheduledThriftTask.getMaxIntervalS());
            Assertions.assertEquals(RoutineLoadJob.DEFAULT_MAX_BATCH_ROWS, scheduledThriftTask.getMaxBatchRows());
            Assertions.assertEquals(RoutineLoadJob.DEFAULT_MAX_BATCH_SIZE, scheduledThriftTask.getMaxBatchSize());
            Assertions.assertEquals(adaptiveTimeoutMs, scheduledTask.getTimeoutMs());

            KafkaTaskInfo nextSchedulingAttempt = new KafkaTaskInfo(new UUID(1, 7), 1L, 20000,
                    taskProgress, false, 1000, false);
            nextSchedulingAttempt.updateAdaptiveTimeout(routineLoadJob);
            TRoutineLoadTask nextThriftTask = new TRoutineLoadTask();
            Deencapsulation.invoke(nextSchedulingAttempt, "adaptiveBatchParam", nextThriftTask, routineLoadJob);
            Assertions.assertEquals(720L, nextThriftTask.getMaxIntervalS());
            Assertions.assertEquals(720L * Config.routine_load_task_timeout_multiplier * 1000L,
                    nextSchedulingAttempt.getTimeoutMs());

            for (int nonPositiveInterval : new int[] {0, -1}) {
                Config.routine_load_adaptive_min_batch_interval_sec = nonPositiveInterval;
                KafkaTaskInfo nonPositiveConfigTask = new KafkaTaskInfo(new UUID(1, 8), 1L, 20000,
                        taskProgress, false, 1000, false);
                nonPositiveConfigTask.updateAdaptiveTimeout(routineLoadJob);
                TRoutineLoadTask nonPositiveConfigThriftTask = new TRoutineLoadTask();
                Deencapsulation.invoke(nonPositiveConfigTask, "adaptiveBatchParam",
                        nonPositiveConfigThriftTask, routineLoadJob);
                Assertions.assertEquals(30L, nonPositiveConfigThriftTask.getMaxIntervalS());
                Assertions.assertEquals(RoutineLoadJob.DEFAULT_MAX_BATCH_ROWS,
                        nonPositiveConfigThriftTask.getMaxBatchRows());
                Assertions.assertEquals(RoutineLoadJob.DEFAULT_MAX_BATCH_SIZE,
                        nonPositiveConfigThriftTask.getMaxBatchSize());
                long normalTimeoutMs = Math.max(30L * Config.routine_load_task_timeout_multiplier,
                        Config.routine_load_task_min_timeout_sec) * 1000L;
                Assertions.assertEquals(normalTimeoutMs, nonPositiveConfigTask.getTimeoutMs());
            }
        } finally {
            Config.routine_load_adaptive_min_batch_interval_sec = previousAdaptiveIntervalSec;
        }
    }

    @Test
    public void testProcessTimeOutTasks() throws Exception {
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        Env env = Mockito.mock(Env.class);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = Mockito.mock(RoutineLoadTaskScheduler.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(routineLoadTaskScheduler);

            RoutineLoadJob routineLoadJob =
                    new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                            1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
            long maxBatchIntervalS = 10;
            Deencapsulation.setField(routineLoadJob, "maxBatchIntervalS", maxBatchIntervalS);

            List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
            Map<Integer, Long> partitionIdsToOffset = Maps.newHashMap();
            partitionIdsToOffset.put(100, 0L);
            KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L,
                    maxBatchIntervalS * 2 * 1000, partitionIdsToOffset, false, -1, false);
            kafkaTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis() - maxBatchIntervalS * 2 * 1000 - 1);
            routineLoadTaskInfoList.add(kafkaTaskInfo);

            Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

            routineLoadJob.processTimeoutTasks();

            List<RoutineLoadTaskInfo> idToRoutineLoadTask =
                    Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
            Assertions.assertNotEquals("1", idToRoutineLoadTask.get(0).getId());
            Assertions.assertEquals(1, idToRoutineLoadTask.size());
        }
    }

    @Test
    public void testFromCreateStmt() throws UserException {
        Env env = Mockito.mock(Env.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<KafkaUtil> kafkaUtilStatic = Mockito.mockStatic(KafkaUtil.class)) {

            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(internalCatalog);

            long dbId = 1L;
            long tableId = 2L;

            Mockito.doReturn(database).when(internalCatalog).getDbOrDdlException(dbName);
            Mockito.doReturn(database).when(internalCatalog).getDbOrAnalysisException(dbName);
            Mockito.doReturn(table).when(database).getOlapTableOrDdlException(tableNameString);
            Mockito.doReturn(table).when(database).getTableOrAnalysisException(tableNameString);
            Mockito.when(database.getId()).thenReturn(dbId);
            Mockito.when(table.getId()).thenReturn(tableId);
            Mockito.when(table.getType()).thenReturn(Table.TableType.OLAP);
            Mockito.doReturn(Mockito.mock(Partition.class)).when(table)
                    .getPartition(Mockito.anyString(), Mockito.anyBoolean());

            kafkaUtilStatic.when(() -> KafkaUtil.getAllKafkaPartitions(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyMap(), Mockito.nullable(String.class)))
                    .thenReturn(Lists.newArrayList(1, 2, 3));

            kafkaUtilStatic.when(() -> KafkaUtil.getRealOffsets(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyMap(), Mockito.anyList(),
                    Mockito.nullable(String.class)))
                    .thenAnswer(invocation -> {
                        List<Pair<Integer, Long>> pairList = new ArrayList<>();
                        pairList.add(Pair.of(1, 0L));
                        pairList.add(Pair.of(2, 0L));
                        pairList.add(Pair.of(3, 0L));
                        return pairList;
                    });

            CreateRoutineLoadInfo createRoutineLoadInfo = initCreateRoutineLoadInfo();
            createRoutineLoadInfo.validate(connectContext);
            RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, null, partitionNames, null,
                    LoadTask.MergeType.APPEND, sequenceColumnName);
            Deencapsulation.setField(createRoutineLoadInfo, "routineLoadDesc", routineLoadDesc);
            List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
            List<PartitionInfo> kafkaPartitionInfoList = Lists.newArrayList();
            for (String s : kafkaPartitionString.split(",")) {
                partitionIdToOffset.add(Pair.of(Integer.valueOf(s), 0L));
                PartitionInfo partitionInfo = new PartitionInfo(topicName, Integer.valueOf(s), null, null, null);
                kafkaPartitionInfoList.add(partitionInfo);
            }
            KafkaDataSourceProperties dsProperties = new KafkaDataSourceProperties(null);
            dsProperties.setKafkaPartitionOffsets(partitionIdToOffset);
            Deencapsulation.setField(dsProperties, "brokerList", serverAddress);
            Deencapsulation.setField(dsProperties, "topic", topicName);
            Deencapsulation.setField(createRoutineLoadInfo, "dataSourceProperties", dsProperties);

            KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateInfo(createRoutineLoadInfo, connectContext);
            Assertions.assertEquals(jobName, kafkaRoutineLoadJob.getName());
            Assertions.assertEquals(dbId, kafkaRoutineLoadJob.getDbId());
            Assertions.assertEquals(tableId, kafkaRoutineLoadJob.getTableId());
            Assertions.assertEquals(serverAddress, Deencapsulation.getField(kafkaRoutineLoadJob, "brokerList"));
            Assertions.assertEquals(topicName, Deencapsulation.getField(kafkaRoutineLoadJob, "topic"));
            List<Integer> kafkaPartitionResult = Deencapsulation.getField(kafkaRoutineLoadJob, "customKafkaPartitions");
            Assertions.assertEquals(kafkaPartitionString, Joiner.on(",").join(kafkaPartitionResult));
            Assertions.assertEquals(sequenceColumnName, kafkaRoutineLoadJob.getSequenceCol());
        }
    }

    private CreateRoutineLoadInfo initCreateRoutineLoadInfo() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), topicName);
        customProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), serverAddress);
        customProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), kafkaPartitionString);

        LoadSeparator loadSeparator = new LoadSeparator(",");
        Map<String, LoadProperty> loadPropertyMap = new HashMap<>();
        loadPropertyMap.put(loadSeparator.getClass().getName(), loadSeparator);
        CreateRoutineLoadInfo createRoutineLoadInfo = new CreateRoutineLoadInfo(labelNameInfo, tableNameString,
                loadPropertyMap, properties, typeName, customProperties, LoadTask.MergeType.APPEND, "");
        return createRoutineLoadInfo;
    }
}
