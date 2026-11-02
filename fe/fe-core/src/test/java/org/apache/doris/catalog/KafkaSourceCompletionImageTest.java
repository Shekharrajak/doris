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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.load.routineload.kafka.KafkaSourceCompletionLedger;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.OffsetRange;
import org.apache.doris.load.routineload.kafka.KafkaSourceDescriptor.Partition;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.meta.MetaFooter;
import org.apache.doris.persist.meta.MetaHeader;
import org.apache.doris.persist.meta.MetaIndex;
import org.apache.doris.persist.meta.MetaPersistMethod;
import org.apache.doris.persist.meta.MetaReader;
import org.apache.doris.persist.meta.PersistMetaModules;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class KafkaSourceCompletionImageTest {
    private static final String MODULE = "kafkaSourceCompletions";
    private static final long CHECKSUM = 17;

    @TempDir
    private Path temporary;

    private boolean originalTracking;
    private boolean originalIgnoreUnknown;
    private int originalMaxPartitions;
    private int originalMaxRanges;
    private long originalMaxIdentityBytes;
    private int originalMaxPending;
    private int originalClusterId;
    private MetaContext originalMetaContext;

    @BeforeEach
    public void setUp() {
        originalTracking = Config.enable_kafka_source_completion_tracking;
        originalIgnoreUnknown = Config.ignore_unknown_metadata_module;
        originalMaxPartitions = Config.kafka_source_completion_max_partitions;
        originalMaxRanges = Config.kafka_source_completion_max_ranges;
        originalMaxIdentityBytes = Config.kafka_source_completion_max_identity_bytes;
        originalMaxPending = Config.kafka_source_completion_max_pending_transactions;
        originalClusterId = Config.cluster_id;
        originalMetaContext = MetaContext.get();
        Config.enable_kafka_source_completion_tracking = false;
        Config.ignore_unknown_metadata_module = false;
        Config.kafka_source_completion_max_partitions = 100;
        Config.kafka_source_completion_max_ranges = 100;
        Config.kafka_source_completion_max_identity_bytes = 10000;
        Config.kafka_source_completion_max_pending_transactions = 8;
        Config.cluster_id = -1;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_kafka_source_completion_tracking = originalTracking;
        Config.ignore_unknown_metadata_module = originalIgnoreUnknown;
        Config.kafka_source_completion_max_partitions = originalMaxPartitions;
        Config.kafka_source_completion_max_ranges = originalMaxRanges;
        Config.kafka_source_completion_max_identity_bytes = originalMaxIdentityBytes;
        Config.kafka_source_completion_max_pending_transactions = originalMaxPending;
        Config.cluster_id = originalClusterId;
        if (originalMetaContext == null) {
            MetaContext.remove();
        } else {
            originalMetaContext.setThreadLocalInfo();
        }
    }

    private KafkaSourceDescriptor source(long namespace, long offset) {
        return new KafkaSourceDescriptor(namespace, "source-cluster", Collections.singletonList(
                new Partition(1, 2, 0, Collections.singletonList(new OffsetRange(offset, offset)))));
    }

    private byte[] saveModule(Env env) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (CountingDataOutputStream output = new CountingDataOutputStream(bytes)) {
            Assertions.assertEquals(CHECKSUM, env.saveKafkaSourceCompletions(output, CHECKSUM));
        }
        return bytes.toByteArray();
    }

    private File writeImage(Path directory, Env env, String module) throws IOException {
        Files.createDirectories(directory);
        File image = directory.resolve("image.7").toFile();
        long offset = MetaHeader.write(image);
        List<MetaIndex> indices = new ArrayList<>();
        long checksum;
        try (CountingDataOutputStream output = new CountingDataOutputStream(
                new FileOutputStream(image, true), offset)) {
            indices.add(new MetaIndex("header", output.getCount()));
            checksum = env.saveHeader(output, 7, 0);
            if (module != null) {
                indices.add(new MetaIndex(module, output.getCount()));
                checksum = env.saveKafkaSourceCompletions(output, checksum);
                indices.add(new MetaIndex("transactionState", output.getCount()));
                checksum = env.saveTransactionState(output, checksum);
            }
        }
        MetaFooter.write(image, indices, checksum);
        return image;
    }

    @Test
    public void testModuleRegistryLoadsCompletionsBeforeTransactions() throws Exception {
        int position = PersistMetaModules.MODULE_NAMES.indexOf(MODULE);
        Assertions.assertTrue(position >= 0);
        Assertions.assertEquals("transactionState", PersistMetaModules.MODULE_NAMES.get(position + 1));
        MetaPersistMethod method = PersistMetaModules.MODULES_MAP.get(MODULE);
        Assertions.assertEquals(Env.class.getDeclaredMethod("loadKafkaSourceCompletions",
                DataInputStream.class, long.class), method.readMethod);
        Assertions.assertEquals(Env.class.getDeclaredMethod("saveKafkaSourceCompletions",
                CountingDataOutputStream.class, long.class), method.writeMethod);
    }

    @Test
    public void testModuleMethodsSkipEmptyAndMergeWhileTrackingDisabled() throws Exception {
        Env imageEnv = new Env(true);
        for (boolean enabled : new boolean[] {false, true}) {
            Config.enable_kafka_source_completion_tracking = enabled;
            Assertions.assertEquals(0, saveModule(imageEnv).length);
        }
        Config.enable_kafka_source_completion_tracking = false;
        KafkaSourceDescriptor first = source(1, 3);
        KafkaSourceDescriptor second = source(2, 5);
        imageEnv.getKafkaSourceCompletionLedger().replayCommitted(first);
        Env loaded = new Env(true);
        loaded.getKafkaSourceCompletionLedger().replayCommitted(second);
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(saveModule(imageEnv)))) {
            Assertions.assertEquals(CHECKSUM, loaded.loadKafkaSourceCompletions(input, CHECKSUM));
        }
        Assertions.assertEquals(Arrays.asList(first, second), loaded.getKafkaSourceCompletionLedger().snapshot());
    }

    @Test
    public void testCor1ImageDispatchAndIndependentEnvOwners() throws Exception {
        Env imageEnv = new Env(true);
        KafkaSourceDescriptor first = source(1, 3);
        KafkaSourceDescriptor second = source(2, 5);
        imageEnv.getKafkaSourceCompletionLedger().replayCommitted(first);
        Path directory = temporary.resolve("complete");
        writeImage(directory, imageEnv, MODULE);
        Env checkpointEnv = new Env(true);
        checkpointEnv.loadImage(directory.toString());
        Assertions.assertNotSame(imageEnv.getKafkaSourceCompletionLedger(),
                checkpointEnv.getKafkaSourceCompletionLedger());
        Assertions.assertEquals(Optional.of(first), checkpointEnv.getKafkaSourceCompletionLedger().getCompleted(first));
        checkpointEnv.getKafkaSourceCompletionLedger().replayCommitted(second);
        Assertions.assertFalse(imageEnv.getKafkaSourceCompletionLedger().getCompleted(second).isPresent());
        Assertions.assertEquals(Optional.of(second),
                checkpointEnv.getKafkaSourceCompletionLedger().getCompleted(second));
    }

    @Test
    public void testEmptyUnknownSectionIsSkippedBeforeModuleLookup() throws Exception {
        Env empty = new Env(true);
        File image = writeImage(temporary.resolve("empty-section"), empty, "unknownKafkaSourceCompletions");
        MetaFooter footer = MetaFooter.read(image);
        Assertions.assertEquals(footer.metaIndices.get(1).offset, footer.metaIndices.get(2).offset);
        Env loaded = new Env(true);
        MetaReader.read(image, loaded);
        Assertions.assertFalse(loaded.getKafkaSourceCompletionLedger().hasCompletions());

        empty.getKafkaSourceCompletionLedger().replayCommitted(source(1, 3));
        File nonempty = writeImage(temporary.resolve("nonempty-unknown"), empty, "unknownKafkaSourceCompletions");
        Assertions.assertThrows(IOException.class, () -> MetaReader.read(nonempty, loaded));
    }

    @Test
    public void testOldAndMissingImagesClearStaleCompletions() throws Exception {
        Env reused = new Env(true);
        KafkaSourceDescriptor stale = source(1, 3);
        reused.getKafkaSourceCompletionLedger().replayCommitted(stale);
        Path oldImage = temporary.resolve("old");
        writeImage(oldImage, new Env(true), null);
        reused.loadImage(oldImage.toString());
        Assertions.assertFalse(reused.getKafkaSourceCompletionLedger().getCompleted(stale).isPresent());

        reused.getKafkaSourceCompletionLedger().replayCommitted(stale);
        Path missingImage = Files.createDirectories(temporary.resolve("missing"));
        reused.loadImage(missingImage.toString());
        Assertions.assertFalse(reused.getKafkaSourceCompletionLedger().hasCompletions());
    }

    @Test
    public void testImageRecoversCompletionsAboveLoweredQuotas() throws Exception {
        Env imageEnv = new Env(true);
        KafkaSourceDescriptor first = source(1, 3);
        KafkaSourceDescriptor second = source(2, 5);
        imageEnv.getKafkaSourceCompletionLedger().replayCommitted(first);
        imageEnv.getKafkaSourceCompletionLedger().replayCommitted(second);
        Path directory = temporary.resolve("lower-quota");
        writeImage(directory, imageEnv, MODULE);
        Config.kafka_source_completion_max_partitions = 1;
        Config.kafka_source_completion_max_ranges = 1;
        Config.kafka_source_completion_max_identity_bytes = 1;
        Env restored = new Env(true);
        restored.loadImage(directory.toString());
        Assertions.assertEquals(Arrays.asList(first, second), restored.getKafkaSourceCompletionLedger().snapshot());
    }

    @Test
    public void testImageResetRejectsPendingNativeTransactions() throws Exception {
        Env env = new Env(true);
        KafkaSourceCompletionLedger ledger = env.getKafkaSourceCompletionLedger();
        KafkaSourceDescriptor completed = source(1, 3);
        ledger.replayCommitted(completed);
        ledger.reserve(7, source(2, 5));
        Path directory = Files.createDirectories(temporary.resolve("pending"));
        Assertions.assertThrows(IllegalStateException.class, () -> env.loadImage(directory.toString()));
        Assertions.assertEquals(Collections.singletonList(completed), ledger.snapshot());
        Assertions.assertEquals(1, ledger.getPendingTransactionCount());
    }
}
