# Step 7: Adaptive Workload-Aware Controller

The adaptive controller is a workload-aware comparison point. It uses more
signals than the reactive lag-threshold baseline:

- expected incoming event rate,
- payload size,
- selected Kafka consumer configuration,
- observed processing capacity per consumer,
- Kafka lag and lag trend,
- consumer p95 end-to-end latency,
- Kafka broker CPU/memory signal from Docker stats.

It still runs as an experiment script in this milestone. Step 8 will move the
scaling actuation into Docker containers.

## Decision Logic

The controller starts `MinConsumers`, then evaluates every
`SampleIntervalSeconds`.

First it selects consumer-side configuration from payload size:

- payload <= 512 bytes: `max.poll.records=500`
- payload <= 4096 bytes: `max.poll.records=200`
- larger payloads: `max.poll.records=50`

Then it estimates required consumers:

```text
required = ceil((incoming_rate * capacity_headroom) / estimated_capacity_per_consumer)
```

The estimate is adjusted upward when lag trend is positive or p95 latency is
above target while lag exists. It is adjusted down to `MinConsumers` after the
producer finishes and lag reaches zero. A cooldown prevents rapid repeated
decisions.

## Run A Smoke Experiment

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-adaptive-controller.ps1 `
  -SkipBuild `
  -RunName adaptive-smoke `
  -MinConsumers 1 `
  -MaxConsumers 2 `
  -InitialCapacityPerConsumer 20 `
  -CapacityHeadroom 1.2 `
  -TargetLatencyP95Millis 3000 `
  -CooldownSeconds 5 `
  -SampleIntervalSeconds 2 `
  -ProducerEventCount 60 `
  -ProducerRatePerSecond 30 `
  -ProducerPayloadSizeBytes 256 `
  -ConsumerProcessingDelayMs 200 `
  -ConsumerRunDurationSeconds 120 `
  -ConsumerReportIntervalMs 2000
```

## Output Layout

```text
results/<run-name>/
  metadata.json
  adaptive-summary.json
  logs/
    consumer-1.out.log
    consumer-2.out.log
    producer.out.log
  samples/
    kafka-lag.jsonl
    adaptive-decisions.jsonl
```

`adaptive-decisions.jsonl` records each evaluation and each start/stop action.
The evaluation rows include desired consumers, active consumers, incoming rate,
payload size, selected `max.poll.records`, estimated capacity per consumer, lag,
lag trend, latency p95, and Kafka resource signals.

`adaptive-summary.json` contains producer summary, selected consumer
configuration, controller settings, all decisions, per-consumer metrics,
aggregate processed/failed counts, peak lag, final lag, and maximum observed
consumer count.

## Current Boundary

This controller is adaptive enough to compare against static and reactive
baselines, but it is still intentionally simple:

- It uses expected producer rate from the experiment configuration rather than
  discovering producer rate from Kafka ingress metrics.
- It applies selected consumer configuration when consumers start.
- It scales host JVMs, not Docker containers.
- It records Kafka broker Docker CPU/memory as a resource signal; per-consumer
  container resource accounting comes in Step 8 when consumers are containerized.

## Verification

Verified on 2026-09-16 with:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-adaptive-controller.ps1 `
  -SkipBuild `
  -RunName adaptive-smoke-fixed `
  -MinConsumers 1 `
  -MaxConsumers 2 `
  -InitialCapacityPerConsumer 20 `
  -CapacityHeadroom 1.2 `
  -TargetLatencyP95Millis 3000 `
  -CooldownSeconds 5 `
  -SampleIntervalSeconds 2 `
  -ProducerEventCount 60 `
  -ProducerRatePerSecond 30 `
  -ProducerPayloadSizeBytes 256 `
  -ConsumerProcessingDelayMs 200 `
  -ConsumerRunDurationSeconds 120 `
  -ConsumerReportIntervalMs 2000
```

The generated `adaptive-summary.json` reported producer exit code `0`, 60
acknowledged events, 60 processed events, 0 consumer failures, selected
`max.poll.records=500` for the 256-byte payload, pre-scaled from 1 to 2
consumers using the workload/capacity estimate, observed peak Kafka lag `14`,
and ended with final Kafka lag `0`.
