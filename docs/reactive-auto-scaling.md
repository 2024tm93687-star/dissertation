# Step 6: Reactive Auto-Scaling

The reactive baseline uses simple threshold rules. It does not estimate workload
shape or choose Kafka configuration; it only reacts to observed Kafka consumer
group lag.

## Controller Rule

The runner starts `MinConsumers`, then samples Kafka group lag every
`SampleIntervalSeconds`.

- Scale up when lag is above `ScaleUpLagThreshold` for
  `ScaleUpConsecutiveSamples` samples and the current count is below
  `MaxConsumers`.
- Scale down after the producer has finished when lag is at or below
  `ScaleDownLagThreshold` for `ScaleDownConsecutiveSamples` samples and the
  current count is above `MinConsumers`.
- A cooldown prevents rapid repeated scaling decisions.

This is deliberately basic. It is the comparison point for the later adaptive
controller, which will use workload rate, payload size, processing capacity,
latency, and resource utilization.

## Run A Reactive Experiment

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-reactive-scaling.ps1 `
  -SkipBuild `
  -RunName reactive-smoke `
  -MinConsumers 1 `
  -MaxConsumers 3 `
  -ScaleUpLagThreshold 8 `
  -ScaleDownLagThreshold 0 `
  -ScaleUpConsecutiveSamples 1 `
  -ScaleDownConsecutiveSamples 2 `
  -CooldownSeconds 5 `
  -ProducerEventCount 80 `
  -ProducerRatePerSecond 40 `
  -ProducerPayloadSizeBytes 256 `
  -ConsumerProcessingDelayMs 200 `
  -ConsumerRunDurationSeconds 120
```

## Output Layout

```text
results/<run-name>/
  metadata.json
  reactive-summary.json
  logs/
    consumer-1.out.log
    consumer-2.out.log
    producer.out.log
  samples/
    kafka-lag.jsonl
    scaling-events.jsonl
```

`reactive-summary.json` contains:

- Producer summary.
- Controller thresholds and cooldown settings.
- Scale events.
- Per-consumer latest metrics.
- Aggregate processed and failed counts.
- Final and peak Kafka lag.
- Maximum observed active consumers.

`scaling-events.jsonl` is the evidence trail for the controller decisions. It
records each consumer start and stop with the active consumer count and reason.

## Current Boundary

Consumers are host JVM processes in this milestone. Scale-down stops extra JVMs
from the runner, which is enough to validate threshold behavior. Step 8 will
move scaling to Docker containers and record rebalance delay more explicitly.

For dissertation comparison, run this against the same workload profiles used by
the static baseline and compare lag, latency, number of consumers, and total
resource usage.

## Verification

Verified on 2026-09-16 with a compact reactive smoke:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-reactive-scaling.ps1 `
  -SkipBuild `
  -RunName reactive-smoke-fixed `
  -MinConsumers 1 `
  -MaxConsumers 2 `
  -ScaleUpLagThreshold 5 `
  -ScaleDownLagThreshold 0 `
  -ScaleUpConsecutiveSamples 1 `
  -ScaleDownConsecutiveSamples 2 `
  -CooldownSeconds 5 `
  -SampleIntervalSeconds 2 `
  -ProducerEventCount 60 `
  -ProducerRatePerSecond 30 `
  -ProducerPayloadSizeBytes 256 `
  -ConsumerProcessingDelayMs 200 `
  -ConsumerRunDurationSeconds 120 `
  -ConsumerReportIntervalMs 2000
```

The generated `reactive-summary.json` reported producer exit code `0`, 60
acknowledged events, 60 processed events, 0 consumer failures, peak Kafka lag
`27`, final Kafka lag `0`, maximum observed consumers `2`, one scale-up event,
and one threshold-based scale-down event.
