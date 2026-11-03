# Experimental Kafka share consumption

Routine Load accepts `"kafka_consumer_type" = "share"` in its `FROM KAFKA`
properties. The default is `standard`, retaining partition assignment and existing
recovery behavior. Upgrade all FEs and BEs before enabling share mode.
The former `kafka_consumer_mode` property remains an alias so existing jobs survive
FE restart; use `kafka_consumer_type` for new jobs. Do not specify both properties.

```sql
CREATE ROUTINE LOAD events_share ON events
PROPERTIES ("desired_concurrent_number" = "9")
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "events",
    "kafka_consumer_type" = "share",
    "property.group.id" = "doris-events-share"
);
```

Use a broker with share groups enabled and compatible with librdkafka 2.16.
Reader concurrency can exceed the number of partitions but remains subject to
Doris task and backend limits. Each reader loads one polled batch per transaction.
Doris keeps a generated group ID across restart when none is supplied. Use a
separate group for each logical destination: members of one group divide records.
Initial position and replay are managed by the broker's share-group configuration.

Delivery is **at least once**: records are accepted only after Doris commits.
Failed tasks attempt to release their acquired batch for retry. A successful
transaction accepts only the records fed into its load pipeline and releases the unprocessed
remainder. An expired acquisition does not prevent acknowledgements for other
records; any acknowledgement failure discards that consumer instead of pooling it.
If release fails or the BE dies, retry depends on broker-side acquisition expiry.
A crash after commit but before acknowledgement, an expired acquisition, or an
acknowledgement failure can duplicate rows. Replay suppression is disabled by
default. Retention and delivery-attempt limits can prevent
retry; load filtering retains the configured Routine Load behavior. Configure
these policies explicitly for correctness experiments.

The experimental implementation supports non-cloud, single-table jobs. Partition
selection, explicit offsets/default offsets, custom acknowledgement mode, AWS IAM,
FILE-backed properties, and source-property changes through ALTER are rejected.
It does not support resetting progress through Doris. Stop/pause does not reset
broker acknowledgement state. Legacy offset-based lag is unavailable in share mode.

BE bvars expose `routine_load_share_consumers`,
`routine_load_share_polling_consumers`, `routine_load_share_received_records`,
`routine_load_share_committed_records`, and `routine_load_share_ack_failures`.
Committed records count source-record attempts, not unique events or output rows.
Polling consumers count consumers in the receive/append phase, not assigned partitions.

For crash testing, `RoutineLoadTaskExecutor.before_share_ack` blocks after Doris
commit and before Kafka acknowledgement. Terminating that BE exercises redelivery.
This implementation makes no exactly-once or performance claim.

## Experimental completion metadata

After upgrading all FEs, set `enable_kafka_source_completion_tracking=true` in
`fe.conf` and restart to retain exact source completion metadata. Only commits
carrying an exact source descriptor are tracked. This does not enable exactly-once
ingestion or change Kafka acknowledgements.

The FE limits source partitions, offset ranges, identity bytes, and pending commits
with `kafka_source_completion_max_partitions`, `kafka_source_completion_max_ranges`,
`kafka_source_completion_max_identity_bytes`, and
`kafka_source_completion_max_pending_transactions`. Capacity exhaustion rejects
new tracked commits before COMMITTED. Quota changes require a restart. Existing
facts survive lower limits, disabled tracking, and transaction/job cleanup; this
phase has no automatic metadata retirement. Missing history remains unknown.

## Experimental completion filtering

With FE completion tracking enabled and all nodes upgraded, set
`enable_kafka_share_completion_filtering=true` in `be.conf` and restart the BEs.
This requires a librdkafka build exposing the delivered message topic UUID API.

The third-party build backports that API to the pinned librdkafka 2.16.0 archive.
Refresh older prebuilt SDK headers and libraries together, then rebuild the BEs:

```bash
./thirdparty/build-thirdparty.sh -j 1 librdkafka
./build.sh --be -j 1
```

The SDK version alone does not establish that the API is present.

Before appending, each batch queries FE for exact completed source identities.
Completed records are acknowledged even if loading new records fails. Unknown
records are loaded once per batch and acknowledged after their transaction commits.
An entirely completed batch aborts its unused transaction without committing rows;
an unconfirmed abort is cleaned up by the FE transaction timeout.

Lookup or identity errors stop the batch before appending. Unknown null payloads
are released for retry; there is no automatic DLQ. Missing completion history stays
unknown. Concurrent transactions can still write the same unknown records, so this
option does not provide exactly-once ingestion.

## Experimental guarded commits

After upgrading all FEs and BEs, enable FE completion tracking and BE
completion filtering as described above. Do not downgrade FEs while guarded jobs
exist. Create a fresh job with an intentional source-start and destination boundary;
completion history does not deduplicate writes from earlier jobs.

```sql
CREATE ROUTINE LOAD events_guarded ON events
PROPERTIES (
    "strict_mode" = "true",
    "max_error_number" = "0",
    "max_filter_ratio" = "0"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "events",
    "kafka_consumer_type" = "share",
    "kafka_share_guarded_commit" = "true",
    "property.group.id" = "doris-events-guarded"
);
```

This creation-only mode rejects overlapping source records before commit. It
requires a supported BE, strict parsing, zero error limits and no WHERE or
PRECEDING FILTER. Failed submissions, load failures and commit conflicts pause
the job. Diagnose the transaction outcome before resuming; uncertain commits keep
their ownership until recovery establishes the durable result. Empty polls retire
their unused transaction and continue normally.

Guarding follows Doris format semantics; a Kafka message need not produce exactly
one row. Blank or empty-array inputs can be parser no-ops, including in mixed
batches. There is no automatic DLQ. Broker retention and delivery-attempt limits
still bound recovery. Native failure-campaign validation remains pending; this is
not a production exactly-once guarantee.

## Kafka trunk regression

Use a disposable non-cloud Doris cluster with debug points enabled, a Kafka trunk
broker with share groups enabled, and a built checkout of that same Kafka revision.
Set `enableKafkaTest = "true"`, `kafkaTrunkHome`, `externalEnvIp`, and `kafka_port`
in the regression configuration's `otherConfigs`, then run:

```bash
./run-regression-test.sh --run -d load_p0/routine_load -s test_kafka_share_at_least_once
```

The suite uses Kafka trunk's `kafka-configs.sh` to configure a fresh share group
with `earliest` and `read_committed`, then tests transaction failure/retry and
pause/resume with more readers than partitions. Its Duplicate Key table preserves
duplicates while the checks require every input ID and its payload to arrive.
The suite skips explicitly unless Kafka tests and `kafkaTrunkHome` are configured.
It uses ordinary share acknowledgements and requires no transactional
acknowledgement extension.

### Guarded recovery regression

Set `enableKafkaShareGuardedTest = "true"` alongside the Kafka settings above.
Use a Docker image built from this branch with `ENABLE_INJECTION_POINT=ON` and
matching librdkafka headers and runtime exposing the delivered-message topic UUID API.
The Kafka advertised addresses must be reachable from both the regression host
and the containers.

```bash
./run-regression-test.sh --run -d load_p0/routine_load \
    -s test_kafka_share_guarded_recovery -dockerSuiteParallel 1 -image <guarded-doris-image>
```

Each fault uses a fresh non-cloud cluster with tracking, filtering and debug
points. The suite stops blocked BEs before the commit RPC, then after commit
but before acknowledgement, and restarts the FE to exercise completion replay.
It requires observed redelivery and exact IDs, payloads and multiplicities in a
Duplicate Key table; missing rows, extra rows and duplicates fail the test.
Faults are mandatory when opted in. This covers two restart windows, not a full
network-partition, journal-loss or performance campaign.

Run the API-enabled native tests separately before merging:

```bash
./run-be-ut.sh -j 1 --run --filter='KafkaShareConsumerTest.*:KafkaShareFilteringTest.*:RoutineLoadTaskExecutorTest.*:BackendGuardedRoutineLoadSubmitTest.*'
```
