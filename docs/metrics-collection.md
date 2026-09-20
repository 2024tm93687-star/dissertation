# Step 4: Metrics Collection

Step 4 turns producer, consumer, Kafka, and Docker observations into repeatable
experiment result folders. The collector is intentionally file-based: each run
produces JSON or JSONL files that can be read later by the adaptive controller,
analysis notebooks, or dissertation tables.

## Run A Collection

Build the producer and consumer once:

```powershell
mvn -B -ntp -f producer/pom.xml package
mvn -B -ntp -f consumer/pom.xml package
```

Then run a small collection:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-experiment.ps1 -SkipBuild
```

The script starts Kafka unless `-SkipKafkaStart` is supplied, starts one
consumer with a fresh group, waits for partition assignment, runs the producer,
samples Kafka lag and Docker resource usage, waits for the consumer's final
snapshot, and writes a summary path to the console. Each run directory must be
new; omit `-RunName` for a timestamped name or provide a unique value.

Use `-ConsumerInstances <n>` to start a fixed number of consumer JVMs in the same
consumer group. Multi-consumer summaries include per-instance final metrics and
an aggregate processed/failed count summary.

For live dashboards during a run, start Prometheus/Grafana and pass
`-EnableConsumerMetricsHttp`. See
[Prometheus and Grafana monitoring](prometheus-grafana-monitoring.md).

## Output Layout

Each run creates:

```text
results/<run-name>/
  metadata.json
  summary.json
  logs/
    consumer.out.log
    consumer.err.log
    producer.out.log
    producer.err.log
  samples/
    kafka-lag.jsonl
    docker-stats.jsonl
```

The log files preserve raw application output. The JSON files are the stable
controller-readable layer.

## Controller-Readable Files

`metadata.json` records the run configuration: topic, group ID, sampling
interval, producer settings, consumer processing settings, and file paths.

`summary.json` contains one object with:

- `schemaVersion`: currently `1`.
- `status`, producer exit code, and consumer exit code.
- `producerSummary`: the final `PRODUCER_SUMMARY` object emitted by the producer.
- `finalConsumerMetrics`: the final or latest `CONSUMER_METRICS` object.
- `finalKafkaLag`: the latest valid lag aggregate from Kafka.
- `lastDockerStats`: the latest valid Docker resource sample.
- `sampleCounts`: number of consumer, lag, and Docker samples captured.

`samples/kafka-lag.jsonl` contains one JSON object per sample. Important fields:

- `timestamp`, `groupId`, and `topic`.
- `partitions`: number of topic partitions observed for the group.
- `currentOffsetSum`: sum of committed offsets when available.
- `logEndOffsetSum`: sum of topic log-end offsets.
- `lag`: aggregate consumer group lag across topic partitions.
- `rows`: per-partition offset and lag detail.

`samples/docker-stats.jsonl` contains one JSON object per sample with raw Docker
stats for currently running containers. CPU and memory values are strings in the
same units reported by Docker, so analysis code should normalize them before
plotting or comparing runs.

## Useful Variations

Count-based workload:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-experiment.ps1 `
  -SkipBuild `
  -RunName static-1c-120events `
  -ProducerEventCount 120 `
  -ProducerRatePerSecond 40 `
  -ProducerPayloadSizeBytes 256 `
  -ConsumerCpuIterations 1000 `
  -ConsumerProcessingDelayMs 0
```

Scenario-file workload:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-experiment.ps1 `
  -SkipBuild `
  -RunName burst-quiet-metrics `
  -ProducerScenarioFile producer/scenarios/burst-quiet.properties `
  -ConsumerRunDurationSeconds 60
```

Heavier processing:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-experiment.ps1 `
  -SkipBuild `
  -RunName heavier-consumer `
  -ProducerEventCount 200 `
  -ProducerRatePerSecond 80 `
  -ConsumerCpuIterations 10000 `
  -ConsumerProcessingDelayMs 1
```

## Current Boundary

This step records evidence; it does not make scaling decisions. Kafka lag,
consumer throughput, end-to-end latency, Docker CPU/memory strings, and producer
rate summaries are now available in one run folder. Step 5 can use this runner
for fixed-consumer baseline experiments. Steps 6 and 7 can reuse the same files
when comparing reactive and adaptive controller behavior.

## Verification

Verified on 2026-09-16 with:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-experiment.ps1 `
  -SkipBuild `
  -RunName step4-smoke-final `
  -ProducerEventCount 10 `
  -ProducerRatePerSecond 10 `
  -ProducerPayloadSizeBytes 256 `
  -ConsumerRunDurationSeconds 30 `
  -ConsumerReportIntervalMs 2000 `
  -SampleIntervalSeconds 2
```

The generated summary reported `status=completed`, producer exit code `0`,
consumer exit code `0`, 10 acknowledged producer events, 10 processed consumer
events, 0 consumer failures, and final Kafka lag `0`.
