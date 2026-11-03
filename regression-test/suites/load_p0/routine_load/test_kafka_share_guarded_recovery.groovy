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

import groovy.json.JsonSlurper
import org.apache.doris.regression.RunMode
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

suite("test_kafka_share_guarded_recovery", "kafka_share,docker") {
    String enabled = context.config.otherConfigs.get("enableKafkaShareGuardedTest")
    if (enabled == null || enabled.equalsIgnoreCase("false")) {
        logger.info("Skipping guarded Kafka recovery: enableKafkaShareGuardedTest is not true")
        return
    }
    assertTrue(enabled.equalsIgnoreCase("true"), "enableKafkaShareGuardedTest must be true or false")
    assertTrue(!context.config.excludeDockerTest, "Guarded recovery requires Docker tests enabled")
    assertTrue(context.config.runMode != RunMode.CLOUD, "Guarded recovery requires native runMode")
    assertTrue(RoutineLoadTestUtils.isKafkaTestEnabled(context), "enableKafkaTest=true is required")
    for (String key : ["kafkaTrunkHome", "externalEnvIp", "kafka_port"]) {
        assertTrue(context.config.otherConfigs.get(key)?.toString()?.trim() as boolean, "${key} is required")
    }
    def kafkaConfigs = new File(context.config.otherConfigs.get("kafkaTrunkHome").toString(), "bin/kafka-configs.sh")
    assertTrue(kafkaConfigs.isFile() && kafkaConfigs.canExecute(), "Kafka trunk kafka-configs.sh is required")
    def broker = RoutineLoadTestUtils.getKafkaBroker(context)
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 2
    options.msNum = 0
    options.recyclerNum = 0
    options.cloudMode = false
    options.enableDebugPoints()
    options.feConfigs += ["enable_kafka_source_completion_tracking=true", "routine_load_task_min_timeout_sec=180"]
    options.beConfigs += ["enable_kafka_share_completion_filtering=true", "sys_log_level=INFO",
            "log_buffer_level=-1"]

    for (def scenario : [
            [name: "precommit", table: "test_kafka_share_guarded_precommit_recovery",
             point: "StreamLoadExecutor.commit_txn.block", committed: false],
            [name: "committed", table: "test_kafka_share_guarded_committed_recovery",
             point: "RoutineLoadTaskExecutor.before_share_ack", committed: true]]) {
        docker(options) {
            def clientProperties = new Properties()
            clientProperties.put("bootstrap.servers", broker)
            def admin = AdminClient.create(clientProperties)
            def producerProperties = new Properties()
            producerProperties.putAll(clientProperties)
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            producerProperties.put("acks", "all")
            producerProperties.put("enable.idempotence", "true")
            producerProperties.put("max.block.ms", "30000")
            producerProperties.put("delivery.timeout.ms", "30000")
            def producer = new KafkaProducer<String, String>(producerProperties)
            def receivedMetric = "routine_load_share_received_records"
            def readReceived = { backend ->
                def (code, out, err) = curl("GET", "http://${backend.Host}:${backend.BrpcPort}/vars/${receivedMetric}")
                assertEquals(0, code, "Cannot read ${receivedMetric}: ${err}")
                def matcher = (out =~ /(?m)^${Pattern.quote(receivedMetric)}\s*:\s*(\d+)$/)
                assertTrue(matcher.find(), "Missing ${receivedMetric}: ${out}")
                matcher.group(1).toLong()
            }
            def backends = { sql_return_maparray("SHOW BACKENDS") }
            def received = { backends().collect { readReceived(it) }.sum() as long }
            def configureGroup = { String group ->
                def log = File.createTempFile("doris-guarded-group-", ".log")
                try {
                    def command = [kafkaConfigs.path, "--bootstrap-server", broker, "--entity-type", "groups",
                            "--entity-name", group, "--alter", "--add-config",
                            "share.auto.offset.reset=earliest,share.isolation.level=read_committed," +
                                    "share.record.lock.duration.ms=30000,share.delivery.count.limit=10"]
                    def process = new ProcessBuilder(command.collect { it.toString() })
                            .redirectErrorStream(true).redirectOutput(log).start()
                    if (!process.waitFor(60, TimeUnit.SECONDS)) {
                        process.destroyForcibly().waitFor(5, TimeUnit.SECONDS)
                        throw new IllegalStateException("Kafka share group configuration timed out")
                    }
                    assertEquals(0, process.exitValue(), log.text)
                } finally {
                    log.delete()
                }
            }
            def sendRows = { String topic, List<List> rows ->
                rows.each { row ->
                    producer.send(new ProducerRecord<String, String>(topic, "${row[0]},${row[1]}".toString()))
                            .get(30, TimeUnit.SECONDS)
                }
                producer.flush()
            }
            def exactRows = { String table ->
                (sql "SELECT id, payload FROM ${table} ORDER BY id, payload").collect {
                    [it[0] as int, it[1].toString()]
                }
            }
            def status = { String job ->
                def rows = sql_return_maparray("SHOW ROUTINE LOAD FOR ${job}")
                assertEquals(1, rows.size())
                def source = new JsonSlurper().parseText(rows[0].DataSourceProperties.toString())
                assertEquals("true", source.kafka_share_guarded_commit?.toString(), "Guarded mode changed: ${rows[0]}")
                rows[0]
            }
            def awaitRows = { String job, String table, List<List> expected ->
                awaitUntil(180, 1) {
                    def current = status(job)
                    assertTrue(current.State in ["NEED_SCHEDULE", "RUNNING"], "Unexpected job state: ${current}")
                    exactRows(table) == expected
                }
                assertEquals(expected, exactRows(table))
            }
            def stopAndStartBackends = {
                def starts = backends().collectEntries { [(it.BackendId.toString()): it.LastStartTime.toString()] }
                assertEquals(2, starts.size())
                // The harness sends stop with a one-second forced-kill fallback while workers are blocked.
                cluster.stopBackends()
                cluster.startBackends()
                awaitUntil(180, 1) {
                    def current = backends()
                    current.size() == starts.size() && current.every {
                        it.Alive.toString().equalsIgnoreCase("true") && starts.containsKey(it.BackendId.toString()) &&
                                starts[it.BackendId.toString()] != it.LastStartTime.toString()
                    }
                }
                backends().each { assertEquals(0L, readReceived(it), "BE received counter did not reset: ${it}") }
            }

            try {
                def suffix = UUID.randomUUID().toString().replace("-", "_")
                def topic = "doris_guarded_${scenario.name}_${suffix}".toString()
                def group = "doris_guarded_${scenario.name}_${suffix}".toString()
                def job = "guarded_${scenario.name}_${suffix}".toString()
                boolean topicCreated = false
                boolean jobCreated = false
                try {
                    admin.createTopics([new NewTopic(topic, 1, (short) 1).configs(
                            ["retention.ms": "3600000", "cleanup.policy": "delete"])])
                            .all().get(30, TimeUnit.SECONDS)
                    topicCreated = true
                    configureGroup(group)
                    sql "DROP TABLE IF EXISTS ${scenario.table}"
                    sql """
                        CREATE TABLE ${scenario.table} (id INT NOT NULL, payload STRING NOT NULL)
                        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES ("replication_num" = "1")
                    """
                    def inputs = [[1, "value_1"]]
                    sendRows(topic, inputs)
                    long baseline = received()
                    def logPositions = cluster.getAllBackends().collectEntries { backend ->
                        def log = new File(backend.getLogFilePath())
                        [(backend.index): log.exists() ? log.text.length() : 0]
                    }
                    GetDebugPoint().enableDebugPointForAllBEs(scenario.point)
                    sql """
                        CREATE ROUTINE LOAD ${job} ON ${scenario.table}
                        COLUMNS TERMINATED BY ","
                        PROPERTIES ("desired_concurrent_number" = "1", "max_batch_interval" = "5",
                            "strict_mode" = "true", "max_error_number" = "0", "max_filter_ratio" = "0")
                        FROM KAFKA ("kafka_broker_list" = "${broker}", "kafka_topic" = "${topic}",
                            "kafka_consumer_type" = "share", "kafka_share_guarded_commit" = "true",
                            "property.group.id" = "${group}")
                    """
                    jobCreated = true
                    def originalJobId = status(job).Id.toString()
                    awaitUntil(120, 1) {
                        def current = status(job)
                        assertTrue(current.State in ["NEED_SCHEDULE", "RUNNING"], "Preflight failed: ${current}")
                        def blocked = cluster.getAllBackends().any { backend ->
                            def log = new File(backend.getLogFilePath())
                            if (!log.exists()) {
                                return false
                            }
                            def text = log.text
                            text.substring(Math.min(logPositions[backend.index] as int, text.length()))
                                    .contains("start debug block ${scenario.point}")
                        }
                        blocked && received() >= baseline + inputs.size()
                    }
                    sql "PAUSE ROUTINE LOAD FOR ${job}"
                    awaitUntil(30, 1) { status(job).State == "PAUSED" }
                    if (scenario.committed) {
                        awaitUntil(120, 1) {
                            assertEquals("PAUSED", status(job).State)
                            exactRows(scenario.table) == inputs
                        }
                        assertEquals(inputs, exactRows(scenario.table))
                    } else {
                        assertEquals([], exactRows(scenario.table), "Rows committed before the pre-RPC barrier")
                    }
                    assertEquals(baseline + inputs.size(), received(),
                            "A replacement reader reacquired input before the recovery fault")
                    if (scenario.committed) {
                        cluster.restartFrontends(1)
                        context.reconnectFe()
                        assertEquals("PAUSED", status(job).State)
                        assertEquals(originalJobId, status(job).Id.toString())
                        assertEquals(inputs, exactRows(scenario.table))
                    }
                    stopAndStartBackends()
                    GetDebugPoint().disableDebugPointForAllBEs(scenario.point)
                    assertEquals("PAUSED", status(job).State)
                    sql "RESUME ROUTINE LOAD FOR ${job}"
                    awaitUntil(180, 1) {
                        def current = status(job)
                        assertTrue(current.State in ["NEED_SCHEDULE", "RUNNING"], "Recovery failed: ${current}")
                        received() >= inputs.size()
                    }
                    awaitRows(job, scenario.table, inputs)
                    // A fresh tail requires a later task after any replay write becomes visible.
                    def tail = (2..25).collect { [it, "value_${it}".toString()] }
                    sendRows(topic, tail)
                    awaitRows(job, scenario.table, inputs + tail)
                    logger.info("Guarded ${scenario.name} recovery preserved exact rows after confirmed BE redelivery")
                } finally {
                    try {
                        GetDebugPoint().disableDebugPointForAllBEs(scenario.point)
                    } finally {
                        try {
                            if (jobCreated) {
                                sql "STOP ROUTINE LOAD FOR ${job}"
                            }
                        } finally {
                            if (topicCreated) {
                                admin.deleteTopics([topic]).all().get(30, TimeUnit.SECONDS)
                            }
                        }
                    }
                }
            } finally {
                try {
                    producer.close(Duration.ofSeconds(10))
                } finally {
                    admin.close(Duration.ofSeconds(10))
                }
            }
        }
    }
}
