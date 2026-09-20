# Step 3: Configurable Telecom Consumer

## Build And Run

This is a Java 21 / Spring Boot 4.1.1 command-line service. It consumes records,
simulates processing, and prints machine-readable metric snapshots. It does not
start an HTTP server. Run these commands from the repository root:

```powershell
mvn -B -ntp -f consumer/pom.xml package
docker-compose -f docker/docker-compose.yml up -d --wait kafka
docker-compose -f docker/docker-compose.yml run --rm kafka-init
java -jar consumer/target/telecom-consumer-0.1.0-SNAPSHOT.jar --debug=false
```

The consumer stays running until Ctrl+C. It prints a `CONSUMER_METRICS` JSON line
every five seconds and a final snapshot on graceful shutdown. Default group
`telecom-consumers` resumes committed offsets. A group with no committed offsets
starts at the earliest retained record, so the first run may read earlier demos
and report large end-to-end latencies.

For Prometheus monitoring, the consumer can also start an Actuator HTTP endpoint
when launched with `--spring.main.web-application-type=servlet`. See
[Prometheus and Grafana monitoring](prometheus-grafana-monitoring.md).

## Try With The Producer

For a fresh demonstration that skips retained history, start the consumer in one
terminal with a new group:

```powershell
$demoGroup = 'telecom-demo-' + [guid]::NewGuid().ToString('N')
java -jar consumer/target/telecom-consumer-0.1.0-SNAPSHOT.jar "--spring.kafka.consumer.group-id=$demoGroup" --spring.kafka.consumer.auto-offset-reset=latest --debug=false
```

Wait until a `CONSUMER_METRICS` line shows `listenerRunning:true` and a nonempty
`assignedPartitions` array. Then run the producer in another terminal:

```powershell
java -jar producer/target/telecom-producer-0.1.0-SNAPSHOT.jar --spring.config.additional-location=file:producer/scenarios/burst-quiet.properties --debug=false
```

The consumer's processed count should increase and interval throughput should
fall during quiet traffic. Offsets are committed after completed poll batches.
Producer acknowledgements and consumer processed counts are different milestones;
wait for consumer processing before comparing them.

For a bounded command-line run, add `--consumer.run-duration=30s`. The timer starts
after Spring startup, not after partition assignment. Graceful shutdown drains
the current poll batch and may take longer than the requested duration. No
processes or containers are automatically scaled by this module.

## Processing Configuration

| Property | Environment variable | Default |
| --- | --- | --- |
| `consumer.topic` | `CONSUMER_TOPIC` | `telecom-events` |
| `consumer.concurrency` | `CONSUMER_CONCURRENCY` | 1 |
| `consumer.cpu-iterations` | `CONSUMER_CPU_ITERATIONS` | 1,000 |
| `consumer.processing-delay-ms` | `CONSUMER_PROCESSING_DELAY_MS` | 0 |
| `consumer.report-interval-ms` | `CONSUMER_REPORT_INTERVAL_MS` | 5,000 |
| `consumer.run-duration` | Use CLI option | Unlimited; optional positive duration up to 1 hour |

CPU iterations perform repeated SHA-256 hashing. The first hash reads the payload;
subsequent hashes read the previous 32-byte digest. The final checksum is retained
to keep the work observable. This is reproducible synthetic CPU work, not a claim
to model real telecom business logic or a fixed processing duration.

The optional delay uses thread sleep and models waiting, not CPU computation.
It may oversleep on Windows. Use CPU work for CPU-saturation experiments and
explicitly describe any waiting component in the experiment definition.

Iterations range from 0 to 1,000,000; delay from 0 to 10,000 ms; concurrency from
1 to 32; reporting interval from 100 to 60,000 ms. Zero iterations and zero delay
leave decoding, validation, Kafka handling, and metric overhead in place.
Concurrency creates Kafka consumer threads inside one JVM. Separate processes
with the same group ID share partitions; different groups each receive the full
topic. Useful concurrent workers cannot exceed available partitions.

## Kafka Configuration

Use the standard `spring.kafka.*` CLI properties. The environment aliases below
are also wired in `consumer/src/main/resources/application.yml`.

| Property | Environment variable | Default |
| --- | --- | --- |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `spring.kafka.consumer.group-id` | `CONSUMER_GROUP_ID` | `telecom-consumers` |
| `spring.kafka.consumer.max-poll-records` | `CONSUMER_MAX_POLL_RECORDS` | 500 |
| `spring.kafka.consumer.properties.fetch.min.bytes` | `CONSUMER_FETCH_MIN_BYTES` | 1 |
| `spring.kafka.consumer.properties.fetch.max.bytes` | `CONSUMER_FETCH_MAX_BYTES` | 52,428,800 |
| `spring.kafka.consumer.properties.max.partition.fetch.bytes` | `CONSUMER_MAX_PARTITION_FETCH_BYTES` | 1,048,576 |
| `spring.kafka.consumer.properties.fetch.max.wait.ms` | `CONSUMER_FETCH_MAX_WAIT_MS` | 500 |
| `spring.kafka.consumer.properties.max.poll.interval.ms` | `CONSUMER_MAX_POLL_INTERVAL_MS` | 300,000 |

For example:

```powershell
java -jar consumer/target/telecom-consumer-0.1.0-SNAPSHOT.jar --spring.kafka.consumer.max-poll-records=50 --consumer.cpu-iterations=5000 --consumer.processing-delay-ms=1 --debug=false
```

Changes apply when the consumer restarts. Auto-commit is explicitly disabled;
attempting to enable it fails startup. The listener uses BATCH acknowledgement
mode with synchronous offset commits. It is a record listener: each record is
processed individually, and commits follow completion of all records from a poll.
The baseline uses the classic group protocol and cooperative sticky assignment.

Ensure worst-case poll-batch processing time stays below `max.poll.interval.ms`.
Increasing `max.poll.records`, hashing work, or sleep delay can otherwise cause
rebalances and replay. Fetch byte settings are not strict process-memory limits;
Kafka may return an oversized first record batch to allow progress. No consumer
Docker CPU/memory limit is applied to this host-JVM milestone.

## Record Validation And Delivery Semantics

Required fields are nonblank `eventId` and `runId`, positive epoch-millisecond
`createdAt`, nonnegative `payloadSizeBytes`, and a nonnull `payload` string whose
UTF-8 byte length matches the declared size. Unknown fields are ignored. Both
earlier producer records and new records with phase metadata are accepted.
Tombstones, invalid JSON, and incomplete events stop the listener container.

The stopped container does not skip or commit past the failed record. The Java
process remains alive so error logs and `listenerRunning:false` can be inspected;
stop it and resolve the bad data before restarting. A bad record is not silently
discarded or sent to a dead-letter topic. The failed counter measures listener
processing failures; broker connectivity or commit errors must also be monitored
through Kafka logs and listener state.

This is **at-least-once** processing. Successful records in an uncommitted batch
may replay after failure or shutdown timeout. Metrics count completed processing
attempts, not deduplicated unique events and not successful commits. Do not label
these measurements exactly-once throughput.

## Metrics

Snapshots include instance ID, group, topic, timestamp, listener state, and:

- `processed`, `failed`, `processedPayloadBytes`, and `processedValueBytes`.
- `intervalThroughputPerSecond`: processed-count change since the previous snapshot divided by monotonic interval time.
- `averageThroughputPerSecond`: processed count divided by time since metrics initialization, including startup and idle time.
- `processingTime`: samples, approximate mean/p95/p99/max in milliseconds for JSON decode, validation, and simulated work.
- `endToEndLatency`: approximate mean/p95/p99/max from producer `createdAt` to processing completion, before offset commit.
- `futureTimestampEvents`: successful records whose producer timestamps are in the future; excluded from latency statistics.
- Assignment, revocation, and lost-partition callback counts plus the current assigned partition list.

Processing time uses `System.nanoTime`; end-to-end latency uses wall-clock epoch
milliseconds, so producer and consumer clocks must agree. Older retained events
include their backlog age in latency. Processing time excludes poll/fetch wait,
commit time, and snapshot logging. End-to-end latency includes producer generation,
buffering, Kafka wait, consumer queueing, and processing.

HdrHistogram stores cumulative per-instance distributions with three significant
digits and microsecond resolution. Recording and snapshot access are synchronized.
The distributions do not reset each reporting interval and must not be averaged
across consumers to obtain a group percentile. Empty distributions report `null`.
Samples outside zero to seven days are excluded and counted in
`excludedOutOfRange`, not clamped to misleading values. No event list or per-run-ID
metric map grows with workload size. These are observed distributions without
coordinated-omission correction.

Callback counts are not group-wide rebalance counts or rebalance duration. Kafka
lag, CPU/memory sampling, live metrics endpoints, group-level aggregation, and
experiment result collection are part of Step 4. A stopped listener can show zero
partitions; an idle or newly starting healthy listener can also temporarily have
none, so evaluate state and logs together.

## Verification

```powershell
mvn -B -ntp -f consumer/pom.xml verify -Pintegration
```

Unit tests cover payload compatibility/validation, hashing, delay, interruption,
configuration, throughput, percentile calculation, clock skew, out-of-range
samples, and assignment tracking. Three live Kafka cases use isolated topics to
check processing/commits/restart, fail-stop behavior on malformed records, and
partition sharing between two application instances. Test topics are removed
afterwards. These are correctness checks, not capacity benchmarks.

Packaged smoke run verified on 2026-09-16 against the local Docker Kafka broker:
one fresh consumer group with `auto-offset-reset=latest`, all six
`telecom-events` partitions assigned, and one producer command sending 40
events at 20 events/second with 256-byte payloads. The final consumer snapshot
reported `processed=40`, `failed=0`, `processedPayloadBytes=10240`, and
`processedValueBytes=22851`.

## References

- [Spring Kafka listener containers and offset commits](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html)
- [Spring Kafka container-stopping error handler](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html)
- [HdrHistogram implementation and precision](https://github.com/HdrHistogram/HdrHistogram)
