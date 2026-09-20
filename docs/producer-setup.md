# Step 2: Telecom Workload Producer

## What Is Implemented

A Java 21 / Spring Boot 4.1.1 command-line application publishes a finite
workload, waits for broker acknowledgements, prints a JSON summary, and exits.
It does not start an HTTP server. Run it from the repository root using the
commands below.

Steady, ramp, burst, quiet-period, and duration-based scenarios are implemented.
See [workload patterns](producer-workload-patterns.md) for ready-to-run scenarios.
Mixed payload sizes remain future work. Count-based steady run length is
approximately `event-count / rate-per-second`.

## Build

```powershell
mvn -B -ntp -f producer/pom.xml clean package
```

This runs unit tests without requiring Kafka and builds
`producer/target/telecom-producer-0.1.0-SNAPSHOT.jar`.

## Run Against Local Kafka

Start Kafka and initialize the topic if necessary:

```powershell
docker-compose -f docker/docker-compose.yml up -d --wait kafka
docker-compose -f docker/docker-compose.yml run --rm kafka-init
```

Publish 100 events at a target of 20 events per second, with 256-byte payloads:

```powershell
java -jar producer/target/telecom-producer-0.1.0-SNAPSHOT.jar --workload.event-count=100 --workload.rate-per-second=20 --workload.payload-size-bytes=256
```

Expect a final `PRODUCER_SUMMARY` line containing `requested:100` and
`acknowledged:100`. The process then exits. `$LASTEXITCODE` should be `0`.
Messages remain in `telecom-events` for later consumers; rerunning adds another
workload with a new run ID.

## Configuration

Append `--property=value` to the Java command, or use Spring Boot environment
variables. For example, `WORKLOAD_RATEPERSECOND=200` corresponds to
`workload.rate-per-second` (Spring Boot removes hyphens in environment names).

| Property | Default | Meaning |
| --- | --- | --- |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Host Kafka listener |
| `workload.topic` | `telecom-events` | Existing destination topic |
| `workload.event-count` | `1000` only when no duration is set | Total events, 1 to 100,000,000; mutually exclusive with duration |
| `workload.pattern` | `steady` | `steady`, `ramp`, or `burst` |
| `workload.duration` | None | Time limit up to one hour; required for changing patterns |
| `workload.rate-per-second` | `100` | Target event starts per second, 1 to 100,000 |
| `workload.payload-size-bytes` | `1024` | Payload field only, 0 to 262,144 bytes |
| `workload.seed` | `42` | Reproducible pseudorandom payload sequence |
| `workload.max-pending-sends` | `256` | Bound on outstanding acknowledgements, 1 to 1,024 |
| `workload.run-id` | Generated UUID | Optional 1-64 character identifier using letters, digits, `_`, `-` |

Choose a new run ID for every experiment. Reusing a run ID also reuses event IDs;
Kafka producer idempotence does not deduplicate separate application runs.
Topic names must begin with a letter or digit and use letters, digits, `.`, `_`,
or `-`, up to 249 characters.

When later running the producer on the Compose network, change the bootstrap
address to `kafka:19092`. A producer Docker image is not included in this milestone.

## Event Contract

Each record is UTF-8 JSON with these fields:

```json
{
  "eventId": "example-run:0",
  "runId": "example-run",
  "sequence": 0,
  "eventType": "CALL_RECORD",
  "createdAt": 1789537632000,
  "subscriberId": "subscriber-0",
  "customerRegion": "north",
  "payloadSizeBytes": 8,
  "payload": "aB3dE6gH",
  "workloadPhase": "steady",
  "workloadElapsedNanos": 123456
}
```

The example is illustrative. Sequence numbers begin at zero; `eventId` combines
run ID and sequence. `createdAt` is epoch milliseconds immediately before payload
generation, so future end-to-end latency includes generation, serialization,
producer buffering, broker time, and consumer processing.

Payloads use seeded pseudorandom ASCII letters and digits. The payload string's
UTF-8 size exactly matches `payloadSizeBytes`. Total Kafka value size is larger
because JSON metadata is additional. `serializedValueBytes` in the summary sums
acknowledged JSON value bytes, excluding keys, Kafka protocol overhead, and
replication. Identical seed and payload size reproduce payloads and subscriber
selection, but timestamps and automatically generated run IDs vary.

The Kafka key is `subscriberId`, cycling over 1,000 synthetic subscribers. Kafka
hashes these keys across partitions; this is not a uniform-partition guarantee.
All events are synthetic call records, with regions cycling north/south/east/west.

## Timing And Delivery

- A monotonic clock controls pacing; wall-clock timestamps are only for event time.
- The first event starts immediately when traffic is active. Steady deadlines advance by a fixed interval rather than from each actual wake-up time; ramp intervals vary with the sampled target rate.
- Delays shorter than one interval are corrected with shorter subsequent waits. This controls average pacing, not a strict instantaneous rate ceiling.
- If an event starts at least one interval late, the schedule resets from that start. Missed intervals are not replayed as a backlog.
- Async sends have a configurable pending-count limit plus a 16 MiB Kafka buffer.
- `acks=all` and producer idempotence are enabled; compression is disabled.
- Startup resolves topic metadata before starting the summary's elapsed interval.
- The elapsed interval includes generation, pacing, sending, and final acknowledgements.
- A send failure stops the run with a nonzero process exit; no success summary is printed.
- Partial delivery can occur on failed or interrupted runs. Use a fresh run ID when retrying.

`acknowledgedRatePerSecond` is acknowledged count divided by elapsed seconds. It
is not consumer throughput or end-to-end latency. Very short runs can exceed the
nominal rate in this average because the first event starts immediately. Sustained
actual rate may fall below target because of Windows scheduling, payload generation,
resource limits, or broker backpressure. A configured rate is not measured arrival
rate; later metrics collection must measure it separately.

The summary also exposes pacing diagnostics:

| Field | Meaning |
| --- | --- |
| `eventStartSpanSeconds` | Monotonic time between first and last event-generation starts |
| `eventStartRatePerSecond` | `(sent - 1) / eventStartSpanSeconds`; `null` for fewer than two events |
| `pacingResetCount` | Number of starts at least one interval late, causing a schedule reset |
| `maxPacingLatenessMillis` | Largest lateness relative to a scheduled start |

The event-start rate excludes the final send and acknowledgement wait, but
includes any earlier generation, serialization, buffering, or scheduling delays.
It measures producer event starts, not broker arrivals or consumer processing.
The basic pacing counters use constant memory and do not introduce a busy-wait
loop. Duration runs also report phases and up to 3,600 one-second windows; see
[workload patterns](producer-workload-patterns.md) for their definitions.

## Repeat Pacing Calibration

After building the current producer and starting Kafka, run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File producer/calibrate-pacing.ps1
```

The script runs two repetitions each at 20 and 50 events/second, 201 events per
run, with 256-byte payloads. Each run uses a fresh JVM and run ID. It checks that
every event was acknowledged and saves raw summaries and signed event-start rate
errors in `producer/target/pacing-calibration-<timestamp>.json`. Results are
checkpointed after each completed run. The `withinTenPercent` field is a diagnostic
flag, not a statistical confidence interval or automatic benchmark approval.

Each default calibration adds 804 synthetic messages to `telecom-events`.
Rates, repetitions, event count, payload size, bootstrap address, and topic can
be set through the script parameters. For example:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File producer/calibrate-pacing.ps1 -Rates 100 -Repetitions 2 -EventCount 1001
```

Higher rates may exceed the host's timing resolution or available processing
capacity. Use measured rates and reset counts to identify unsuitable settings.
These runs include cold payload/serialization initialization and have no warm-up
phase; define warm-up and longer measurement windows before dissertation experiments.

## Live Integration Test

With the broker running:

```powershell
mvn -B -ntp -f producer/pom.xml verify -Pintegration
```

This runs unit tests and four real-broker cases through `localhost:9092`: count,
timed steady, ramp, and burst with quiet/recovery. Each uses its own temporary
topic and checks consumed count, unique IDs, payloads, phase membership, and
timing. Duration cases reconcile phase/window summaries with consumed records.
Test topics are removed even on assertion failure. This is functional verification,
not a performance benchmark or the dissertation's consumer implementation.

## Troubleshooting

- Connection refused or metadata timeout: check Docker and `docker-compose -f docker/docker-compose.yml ps`.
- Unknown topic: run `kafka-init`; topic auto-creation is disabled in this environment.
- Validation error: check the ranges above; invalid settings fail before publishing.
- Invalid pattern configuration: see the required duration/phase options in [workload patterns](producer-workload-patterns.md). Unknown options fail explicitly.
- Maven download failure: check access to Maven Central. Kafka is not needed for unit tests.

## Verified Results

On 2026-09-16 using Java 21.0.12.1, Spring Boot 4.1.1, its managed Kafka client
4.2.1, and the local Kafka 4.2.0 broker:

- `mvn -B -ntp -f producer/pom.xml verify -Pintegration`: passed all 13 unit tests and 1 live integration test.
- The integration test verified all 30 records and removed its temporary topic.
- The packaged application published 100 events with 256-byte payloads to `telecom-events` and exited with code 0.
- Run ID: `c6d20fd7-0c14-4f57-9fc1-056702bb6ea9`; requested 100, acknowledged 100.
- Target rate: 20 events/second; measured acknowledgement rate: 15.34 events/second over 6.52 seconds.
- Total acknowledged serialized JSON value bytes: 51,320.

These are historical smoke-test observations before the pacing correction, not
dissertation benchmark results. The sample events remain in `telecom-events`
subject to Kafka retention. See [pacing calibration](producer-pacing-calibration.md)
for the subsequent investigation and verification.

## References

- [Spring Boot system requirements](https://docs.spring.io/spring-boot/system-requirements.html)
- [Spring Boot Kafka support](https://docs.spring.io/spring-boot/reference/messaging/kafka.html)
- [Spring Kafka sending messages](https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html)
