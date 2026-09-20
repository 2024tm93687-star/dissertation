# Step 8: Docker Consumer Scaling

Step 8 moves scaling actuation from host JVM processes to Docker containers.
The consumer image is built from `consumer/Dockerfile`, and each scaled consumer
container receives Kafka and consumer settings through environment variables.

## Shared Execution Backend

`run-docker-scaling.ps1` is now the common consumer execution backend for all
three comparison policies. Select the policy with `-ControllerMode`:

- `Static` starts `-FixedConsumers` containers and never changes that count.
- `Reactive` changes container count from Kafka lag thresholds.
- `Adaptive` plans initial capacity from workload rate, selects poll/fetch
  settings from payload size, and responds to lag pressure.

The full matrix invokes these modes automatically. Each mode uses the same image,
Docker network, CPU/memory limits, consumer implementation, metrics parser, log
capture, lag sampler, and shutdown path. Only policy decisions differ.

The parity smoke matrix `docker-parity-smoke` verified all three modes with one
CPU and 512 MiB per consumer. All runs reconciled acknowledged and processed
counts, reported zero failures and zero final lag, and passed the evidence gate.
Adaptive also started and stopped a second container during the run.

## Build The Consumer Image

```powershell
mvn -B -ntp -f consumer/pom.xml package
docker build -t dissertation-telecom-consumer:0.1.0 consumer
```

The image runs:

```text
java -jar /app/telecom-consumer.jar
```

## Run A Docker Scaling Smoke

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-docker-scaling.ps1 `
  -SkipBuild `
  -RunName docker-scaling-smoke `
  -MinConsumers 1 `
  -MaxConsumers 2 `
  -ScaleUpLagThreshold 5 `
  -ScaleDownLagThreshold 0 `
  -ProducerEventCount 60 `
  -ProducerRatePerSecond 30 `
  -ProducerPayloadSizeBytes 256 `
  -ConsumerProcessingDelayMs 200 `
  -ConsumerContainerCpus 1.0 `
  -ConsumerContainerMemory 512m
```

The runner starts Kafka, builds the consumer image unless `-SkipImageBuild` is
passed, starts/stops named consumer containers on the `dissertation_default`
Docker network, captures container logs, samples Kafka lag, and writes a summary.

## Verified Smoke Runs

The Docker actuation path was verified on 2026-09-16 with two compact runs:

- `docker-scaling-smoke-fixed`: acknowledged 40 events, processed 40 events,
  recorded 0 failures, and ended with Kafka lag 0.
- `docker-scaling-scaleup-smoke`: acknowledged 20 events, processed 20 events,
  recorded 0 failures, started additional consumer containers through Docker,
  observed a maximum of 2 active consumers, and ended with Kafka lag 0.

The scale-up smoke intentionally used a permissive threshold to prove container
start/stop mechanics. It is a functional check, not a performance benchmark.

## Output Layout

```text
results/<run-name>/
  metadata.json
  docker-scaling-summary.json
  logs/
    <run-name>-consumer-1.log
    <run-name>-consumer-2.log
    producer.out.log
  samples/
    kafka-lag.jsonl
    docker-scaling-events.jsonl
```

`docker-scaling-events.jsonl` records container starts and stops. Stop events
include Docker stop duration, which is a practical proxy for shutdown/rebalance
actuation time in this milestone.

`docker-scaling-summary.json` contains producer summary, scaling events,
per-container consumer metrics parsed from logs, aggregate processed/failed
counts, peak lag, final lag, and maximum observed consumers.

## Configuration Passed To Containers

The runner passes:

- `KAFKA_BOOTSTRAP_SERVERS=kafka:19092`
- `CONSUMER_GROUP_ID`
- `CONSUMER_TOPIC`
- `CONSUMER_CPU_ITERATIONS`
- `CONSUMER_PROCESSING_DELAY_MS`
- `CONSUMER_REPORT_INTERVAL_MS`
- `CONSUMER_MAX_POLL_RECORDS`
- `CONSUMER_FETCH_MIN_BYTES`
- `CONSUMER_MAX_PARTITION_FETCH_BYTES`

It also applies Docker resource limits with `--cpus` and `--memory`.

## Current Boundary

This milestone proves Docker-based actuation and resource limits. The scaling
rule is intentionally simple, similar to the reactive threshold baseline. The
next implementation should run the full static, reactive, and adaptive
experiment matrix using the existing runners so the dissertation has comparable
result sets for analysis.
