# Step 5: Static Baseline Experiments

The static baseline keeps the scaling decision fixed. It runs the same workload
against a fixed number of consumers and records the result files created by the
Step 4 collector. These runs become the comparison point for reactive and
adaptive scaling.

## Run A Small Baseline

Build once, then run the baseline script:

```powershell
mvn -B -ntp -f producer/pom.xml package
mvn -B -ntp -f consumer/pom.xml package

powershell -ExecutionPolicy Bypass -File experiments/run-static-baseline.ps1 `
  -SkipBuild `
  -ConsumerCounts 1,2,4 `
  -Replications 1 `
  -ProducerEventCount 240 `
  -ProducerRatePerSecond 80 `
  -ProducerPayloadSizeBytes 256 `
  -ConsumerRunDurationSeconds 60
```

Each consumer count uses a fresh Kafka consumer group, so retained older topic
records are skipped with `auto-offset-reset=latest` and only the current workload
is measured.

## Output Layout

The script creates one run-set folder:

```text
results/<run-set-name>/
  baseline-manifest.json
  static-c1-r1/
    summary.json
    metadata.json
    logs/
    samples/
  static-c2-r1/
    summary.json
    metadata.json
    logs/
    samples/
  static-c4-r1/
    summary.json
    metadata.json
    logs/
    samples/
```

`baseline-manifest.json` summarizes the comparable runs:

- `consumerInstances`: fixed consumer count for the run.
- `producerAcknowledged`: producer acknowledgements.
- `consumerProcessed`: aggregate processed events across consumer instances.
- `consumerFailed`: aggregate consumer failures.
- `finalLag`: final Kafka group lag.
- `processingP95MillisByInstance`: processing p95 for each consumer JVM.
- `latencyP95MillisByInstance`: end-to-end latency p95 for each consumer JVM.
- `summary`: path to the detailed Step 4 summary file.

## Single Run With Multiple Consumers

The lower-level collector now supports `-ConsumerInstances` directly:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-experiment.ps1 `
  -SkipBuild `
  -RunName fixed-two-consumers `
  -ConsumerInstances 2 `
  -ProducerEventCount 120 `
  -ProducerRatePerSecond 40 `
  -ConsumerRunDurationSeconds 45
```

The detailed `summary.json` includes:

- `consumerExitCodes`: exit code per consumer JVM.
- `consumerInstanceMetrics`: final metrics per consumer JVM.
- `aggregateConsumerMetrics`: summed processed, failed, byte, timestamp, and
  rebalance callback counts.

Latency percentiles are kept per instance because percentiles cannot be summed
or averaged into a correct group percentile. For dissertation plots, use
per-instance percentiles or rerun analysis from raw event-level observations if
a true group percentile is required.

## Dissertation Use

Recommended baseline matrix for the first full pass:

- Consumers: `1`, `2`, `4`.
- Workloads: steady, ramp, and burst/quiet.
- Payload sizes: start with `256` bytes, then add larger payload profiles.
- Replications: at least `3` for dissertation tables after the scripts are
  stable.

Static baseline results should answer: with no automatic scaling or configuration
changes, when does lag begin to grow, how does latency change, and how much
throughput does each added consumer contribute?

## Verification

Verified on 2026-09-16 with a compact smoke baseline:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-static-baseline.ps1 `
  -SkipBuild `
  -RunSetName static-baseline-smoke-fixed `
  -ConsumerCounts 1,2 `
  -Replications 1 `
  -ProducerEventCount 12 `
  -ProducerRatePerSecond 6 `
  -ProducerPayloadSizeBytes 256 `
  -ConsumerRunDurationSeconds 35 `
  -ConsumerReportIntervalMs 2000 `
  -SampleIntervalSeconds 2
```

The generated `baseline-manifest.json` reported completed runs for 1 and 2
fixed consumers. Both runs acknowledged 12 producer events, processed 12
consumer events, had 0 consumer failures, and ended with Kafka lag `0`.
