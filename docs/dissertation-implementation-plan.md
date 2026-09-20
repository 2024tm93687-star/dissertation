# Dissertation Implementation Plan

## Topic

Adaptive Workload-Aware Consumer Scaling and Configuration for Kafka Consumers

## Research Goal

Build and evaluate an adaptive controller that can automatically decide the required Kafka consumer capacity and consumer-side configuration for a changing workload while keeping latency and consumer lag under control and avoiding unnecessary resource usage.

The main research question is:

> For a changing workload, can the system automatically decide the required consumer capacity and Kafka consumer configuration while keeping latency and consumer lag under control and avoiding unnecessary resource usage?

## Proposed System Architecture

The implementation will be organized into the following components.

### 1. Kafka Docker Environment

Purpose:

- Provide a controlled local Kafka environment.
- Run repeatable experiments on a personal computer.
- Apply fixed CPU and memory limits to Kafka brokers and consumers.

Main responsibilities:

- Start Kafka using Docker Compose.
- Create required topics.
- Provide consistent experiment conditions.
- Allow different resource profiles through Docker limits.

Expected files:

```text
docker/
  docker-compose.yml
  kafka-init/
```

### 2. Synthetic Telecom Workload Producer

Purpose:

- Generate repeatable telecom-style event workloads.
- Support different event rates, payload sizes, and burst patterns.

Generated event fields may include:

```json
{
  "eventId": "uuid",
  "eventType": "CALL_RECORD",
  "createdAt": 123456789,
  "payloadSizeBytes": 1024,
  "customerRegion": "north",
  "payload": "..."
}
```

Supported workload patterns:

- Constant low rate.
- Constant high rate.
- Gradual ramp-up.
- Sudden burst.
- Burst followed by quiet period.
- Small messages at high rate.
- Large messages at medium rate.
- Mixed payload sizes.

Expected files:

```text
producer/
```

### 3. Configurable Kafka Consumer

Purpose:

- Consume synthetic telecom events.
- Simulate processing cost.
- Measure throughput, latency, and processing behavior.
- Allow runtime configuration through environment variables.

Configurable Kafka consumer properties:

- `max.poll.records`
- `fetch.min.bytes`
- `fetch.max.bytes`
- `fetch.max.wait.ms`
- `max.partition.fetch.bytes`

Measured metrics:

- Processed message count.
- Consumer throughput.
- End-to-end latency.
- Processing time.
- Rebalance events.
- CPU and memory usage, where available.

Expected files:

```text
consumer/
```

### 4. Adaptive Controller

Purpose:

- Run as a separate Spring Boot service.
- Periodically monitor workload, consumer, Kafka, and resource metrics.
- Decide whether to scale consumers up or down.
- Decide whether consumer configuration should change.
- Apply scaling and configuration changes through Docker.

Metrics monitored by the controller:

- Incoming event rate.
- Average payload size.
- Consumer processing capacity.
- Consumer lag.
- Processing latency.
- CPU utilization.
- Memory utilization.
- Current number of consumers.
- Current number of Kafka partitions.

Kafka information read using `AdminClient`:

- Topics.
- Partitions.
- Consumer groups.
- Committed offsets.
- End offsets.
- Consumer lag.

Expected files:

```text
controller/
```

### 5. Experiment Runner

Purpose:

- Run repeatable experiments for all approaches.
- Apply the same workloads to each approach.
- Save results in structured files for analysis.

Expected files:

```text
experiments/
  scenarios/
  results/
```

### 6. Analysis Layer

Purpose:

- Analyze experiment output.
- Calculate normalized performance metrics.
- Generate graphs and tables for the dissertation.

Expected files:

```text
analysis/
```

## Baseline Approaches

The dissertation will compare three approaches.

### 1. Static Baseline

Fixed number of consumers and fixed Kafka consumer properties.

Examples:

- 1 consumer, fixed `max.poll.records`.
- 2 consumers, fixed `max.poll.records`.
- 4 consumers, fixed `max.poll.records`.

Purpose:

- Establish predictable baseline behavior.
- Show limitations of fixed configuration under changing workloads.

### 2. Reactive Auto-Scaling Baseline

Scale using simple thresholds such as CPU or consumer lag.

Example logic:

```text
if lag > threshold for N intervals:
    scale up

if lag < threshold and CPU is low for N intervals:
    scale down
```

Purpose:

- Represent a common simple scaling strategy.
- Provide a stronger baseline than static configuration.

### 3. Adaptive Workload-Aware Approach

Use workload and performance metrics together:

- Event rate.
- Payload size.
- Processing capacity.
- Lag trend.
- Latency.
- CPU and memory utilization.

Example scaling estimate:

```text
required_consumers =
    ceil(incoming_event_rate / estimated_capacity_per_consumer)
```

Then constrain the result:

```text
required_consumers <= partition_count
required_consumers <= max_allowed_consumers
required_consumers >= min_allowed_consumers
```

Example configuration decisions:

```text
small messages + low processing cost:
    increase max.poll.records

large messages + high latency:
    reduce max.poll.records

high fetch overhead:
    increase fetch.min.bytes or fetch wait

memory pressure:
    reduce batch/fetch size
```

Purpose:

- Provide the main dissertation contribution.
- Show whether workload-aware control improves lag, latency, and resource efficiency.

## Docker-Based Scaling Strategy

Consumer scaling will be performed through Docker.

Options:

1. Use the Docker Engine API from the controller.
2. Use Docker Compose commands externally.

Recommended approach:

- Use the Docker Engine API with container labels.
- Start and stop consumer containers programmatically.
- Pass consumer configuration through environment variables.
- Let Kafka automatically rebalance partitions when consumers join or leave the same consumer group.

## Experiment Scenarios

Recommended scenarios:

```text
S1: steady low workload
S2: steady high workload
S3: gradual ramp-up
S4: sudden traffic burst
S5: burst followed by quiet period
S6: small messages, high rate
S7: large messages, medium rate
S8: mixed payload sizes
```

Each scenario should be run for:

- Static baseline.
- Reactive auto-scaling.
- Adaptive workload-aware controller.

## Metrics To Collect

Core metrics:

- Total throughput.
- Average latency.
- p95 latency.
- p99 latency.
- Consumer lag.
- CPU usage.
- Memory usage.
- Number of active consumers.
- Rebalance duration.
- Scaling decisions.
- Configuration changes.

Kafka-specific metrics:

- Topic partitions.
- End offsets.
- Committed offsets.
- Lag per partition.
- Total consumer group lag.

Controller metrics:

- Estimated incoming rate.
- Estimated processing rate per consumer.
- Required consumer count.
- Actual consumer count.
- Scale-up decisions.
- Scale-down decisions.
- Configuration decision history.

## Normalized Analysis

Since experiments run on a personal computer, results should not be presented only as absolute production capacity. The analysis should include normalized measures.

Recommended formulas:

```text
throughput_per_core =
    total_throughput / allocated_cpu_cores

processing_capacity_per_consumer =
    total_processing_rate / active_consumers

scaling_efficiency =
    throughput_with_n_consumers / (n * throughput_with_1_consumer)

lag_growth_rate =
    (current_lag - previous_lag) / interval_seconds

capacity_gap =
    incoming_rate - processing_rate

over_provisioning_ratio =
    allocated_capacity / observed_workload
```

Useful dissertation graphs:

- Workload rate over time.
- Consumer lag over time.
- Latency over time.
- Number of consumers over time.
- CPU and memory usage over time.
- Throughput per CPU core.
- Scaling efficiency by consumer count.
- Static vs reactive vs adaptive comparison.

## Recommended Repository Structure

```text
dissertation/
  docker/
    docker-compose.yml
  producer/
  consumer/
  controller/
  experiments/
    scenarios/
    results/
  analysis/
  docs/
    dissertation-implementation-plan.md
```

## Step-By-Step Build Order

### Step 1: Create Kafka Docker Environment

Status: completed and verified on 2026-09-16. See the
[local Kafka setup guide](kafka-local-setup.md) for commands, resource limits,
and the successful produce/consume check.

Deliverables:

- Docker Compose file.
- Kafka broker running locally.
- Test topic created.
- Basic produce/consume verification.

### Step 2: Build Synthetic Telecom Producer

First milestone completed and verified on 2026-09-16:
[steady telecom producer](producer-setup.md), with configurable
event count, target rate, payload size, and seed.

Verification: 13 unit tests and 1 live Kafka integration test passed. The packaged
application also published 100 acknowledged events to `telecom-events`.

Pacing follow-up: corrected cumulative scheduling drift, added event-start rate
and delay diagnostics, and added a repeatable calibration script. See
[producer pacing calibration](producer-pacing-calibration.md).

Changing workload milestone completed and verified on 2026-09-16:
[workload patterns](producer-workload-patterns.md) now support duration-based
steady traffic, a linear ramp, burst, quiet interval, and recovery. Phase and
one-second reports compare requested and observed rates. Existing count-based
commands still work; count and duration limits are mutually exclusive.

Verification: 30 unit tests and 4 live Kafka cases passed. Packaged 12-second
ramp and burst/quiet scenarios acknowledged 300 and 179 events respectively;
the quiet phase had zero event starts. These are functional checks, not benchmarks.

Step 3 milestone completed and verified on 2026-09-16:
[configurable consumer](consumer-setup.md) now provides a Spring Boot Kafka
consumer with configurable poll/fetch settings, simulated CPU and delay cost,
strict event validation, at-least-once offset commits, and machine-readable
throughput/latency metrics. Verification passed 17 unit tests, 3 live Kafka
integration cases, and a packaged producer-to-consumer smoke run that processed
40 events with 0 failures.

Step 4 milestone completed and verified on 2026-09-16:
[metrics collection](metrics-collection.md) now provides a repeatable experiment
runner that captures producer summaries, consumer metric snapshots, Kafka group
lag samples, Docker resource samples, run metadata, and a controller-readable
`summary.json` result file. A packaged smoke run completed with producer and
consumer exit code 0, 10 acknowledged events, 10 processed events, 0 consumer
failures, and final Kafka lag 0.

Monitoring extension added: [Prometheus and Grafana monitoring](prometheus-grafana-monitoring.md)
uses Micrometer/Actuator for custom consumer metrics, Prometheus for scraping,
a Kafka exporter for consumer-group lag, and a provisioned Grafana dashboard for
live experiment observation.

Step 5 static baseline framework completed and verified on 2026-09-16:
[static baseline experiments](static-baseline-experiments.md) now provides a
fixed-consumer baseline runner that executes repeatable runs for selected
consumer counts, stores full Step 4 result folders, and writes a
`baseline-manifest.json` comparison summary. A compact verification run completed
1-consumer and 2-consumer baselines with 12 acknowledged events, 12 processed
events, 0 consumer failures, and final Kafka lag 0 for both runs. The larger
dissertation matrix across all workload scenarios should be collected before
final analysis.

Step 6 reactive auto-scaling framework completed and verified on 2026-09-16:
[reactive auto-scaling](reactive-auto-scaling.md) now provides a simple
threshold controller that samples Kafka group lag, scales consumer JVMs between
minimum and maximum bounds, records scaling decisions, and writes a
`reactive-summary.json` result file. A compact verification run acknowledged 60
events, processed 60 events, had 0 consumer failures, observed peak Kafka lag
27, scaled from 1 to 2 consumers, scaled back down after lag reached 0, and
ended with final Kafka lag 0.

Step 7 adaptive controller framework completed and verified on 2026-09-16:
[adaptive controller](adaptive-controller.md) now provides a workload-aware
controller that selects consumer-side configuration from payload size, estimates
required consumers from incoming rate and observed processing capacity, adjusts
for lag trend and p95 latency, records Kafka resource signals, and writes an
`adaptive-summary.json` result file. A compact verification run acknowledged 60
events, processed 60 events, had 0 consumer failures, selected
`max.poll.records=500` for 256-byte payloads, pre-scaled from 1 to 2 consumers,
observed peak Kafka lag 14, and ended with final Kafka lag 0.

Step 8 Docker scaling framework completed and verified on 2026-09-16:
[Docker consumer scaling](docker-scaling.md) now provides a consumer image,
Docker Compose service definition, and Docker-based scaling runner that starts
and stops consumer containers with CPU/memory limits and environment-driven
consumer configuration. Compact verification runs acknowledged 40 and 20 events,
processed all acknowledged events with 0 failures, demonstrated Docker
start/stop actuation, observed up to 2 active consumers in the scale-up smoke,
and ended with final Kafka lag 0.

Step 9 experiment orchestration implemented and smoke-verified on 2026-09-16. The
[full experiment matrix guide](full-experiment-matrix.md) provides smoke and
dissertation presets, runs every selected approach against the same scenario
files, supports replications, and writes a consolidated `matrix-manifest.json`.
The four-approach smoke matrix completed all four runs. An initial static run
exposed reused consumer-group offsets; matrix-specific group isolation and an
automatic evidence-quality gate were then added. The corrected static validation
acknowledged and processed 234 events with 0 failures, final lag 0, and
`evidenceReady=true`.

Step 10 analysis tooling implemented and verified on 2026-09-16. The
[results analysis guide](results-analysis.md) documents a dependency-free
PowerShell analyzer that produces per-run and aggregate CSV tables, a Markdown
report, and SVG charts for normalized throughput, p95 latency, and peak lag. It
excludes runs that fail the evidence-quality gate by default. Verification
successfully analyzed the clean isolated result and generated a four-approach
diagnostic report from the smoke matrix, including all three SVG charts.

Final experiment protocol draft prepared on 2026-09-16. See the
[final experiment protocol](final-experiment-protocol.md) for the versioned
settings, four 60-second payload-diverse workloads, validity gate, environment
capture process, and controlled-run procedure. Static, reactive, and adaptive
matrix runs now use one Docker consumer backend with identical image, CPU,
memory, network, metric, and lifecycle behavior. The corrected full design has
60 runs because each of the four scenarios includes three static consumer counts
and two dynamic approaches across three replications.

Docker execution parity was verified on 2026-09-16 with the
`docker-parity-smoke` matrix. Static, reactive, and adaptive each completed using
1 CPU and 512 MiB consumer containers; all acknowledged records were processed,
failures and final lag were zero, and all three runs passed the evidence gate.
The Step 10 analyzer also accepted the new common summary format.

Seeded run-order randomization and checkpointed resume were implemented and
verified on 2026-09-17. Two independently generated dissertation schedules had
the same 60 unique entries for seed `20260917`. The randomized Docker smoke ran
adaptive, static, then reactive; all three results were evidence-ready. Resume
skipped all completed entries without creating new attempts. Protocol version
1 is now frozen.

Next implementation step: commit the frozen implementation, capture a clean
environment record, and run the final 60-run dissertation matrix under the
documented controlled conditions.

Deliverables:

- Producer service.
- Configurable event rate.
- Configurable payload size.
- Configurable workload pattern.

### Step 3: Build Configurable Consumer

Deliverables:

- Consumer service.
- Configurable Kafka consumer properties.
- Simulated processing cost.
- Latency and throughput measurement.

### Step 4: Add Metrics Collection

Deliverables:

- Consumer metrics endpoint or log output.
- Producer metrics.
- Controller-readable metric format.
- Experiment result files.

### Step 5: Implement Static Baseline

Deliverables:

- Scripts to run fixed-consumer experiments.
- Result files for each workload scenario.

### Step 6: Implement Reactive Auto-Scaling

Deliverables:

- Simple threshold-based controller mode.
- Scale-up and scale-down based on lag or CPU threshold.
- Result files for comparison.

### Step 7: Implement Adaptive Controller

Deliverables:

- Workload-aware decision loop.
- Consumer capacity estimation.
- Lag trend analysis.
- Latency-aware scaling.
- Consumer configuration selection.

### Step 8: Implement Docker Scaling

Status: completed and verified on 2026-09-16. See the
[Docker consumer scaling guide](docker-scaling.md) for image build commands,
runner usage, output layout, and smoke-test results.

Deliverables:

- Start new consumer containers.
- Stop excess consumer containers.
- Restart consumers with updated configuration.
- Track rebalance delay.

### Step 9: Run Full Experiments

Status: matrix runner and smoke workflow verified on 2026-09-16. Final
dissertation benchmark collection remains pending and should be performed under
stable machine and Docker resource conditions.

Deliverables:

- Static baseline results.
- Reactive auto-scaling results.
- Adaptive workload-aware results.
- Repeatable experiment logs.

### Step 10: Analyze Results

Status: completed and smoke-verified on 2026-09-16. Final statistical conclusions
remain pending until the dissertation matrix has been collected.

Deliverables:

- Normalized metrics.
- Graphs.
- Tables.
- Written interpretation of trade-offs.

### Step 11: Prepare Dissertation Write-Up

Suggested sections:

- Introduction.
- Background on Kafka consumer scaling.
- Problem statement.
- Proposed adaptive controller.
- System design and implementation.
- Experimental setup.
- Results.
- Analysis.
- Threats to validity.
- Conclusion and future work.

## Main Dissertation Contribution

The main value addition is the combination of:

- An adaptive controller for Kafka consumer scaling and configuration.
- A comparison against clear static and reactive baselines.
- Resource-normalized analysis that makes local-machine experiments useful for understanding broader scaling trends.
