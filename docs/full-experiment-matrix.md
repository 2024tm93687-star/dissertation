# Full Experiment Matrix

Step 9 provides one orchestrator for running identical workload scenarios against
the static, reactive, and adaptive implementations. Every approach uses the same
Docker consumer image and per-container CPU/memory limits. It preserves
each runner's detailed files and writes a consolidated `matrix-manifest.json` for
the analysis stage.

## Presets

`Smoke` runs `steady` once, with one static consumer. Use it after code changes:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-full-experiment-matrix.ps1 `
  -Preset Smoke `
  -MatrixName matrix-smoke
```

`Dissertation` runs the four 60-second scenarios in `experiments/scenarios`, with
three replications. The static baseline uses 1, 2, and 4 consumers. Reactive and
adaptive approaches each run three times per scenario:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-full-experiment-matrix.ps1 `
  -Preset Dissertation `
  -MatrixName dissertation-main
```

This is 60 experiment runs and can take substantial time. Close unrelated heavy
applications, keep the computer connected to power, and do not compare runs made
while Docker resource settings or background load differ.

The runner writes a deterministic randomized schedule before execution. Use
`-RandomSeed 20260917` for protocol version 1. `-ScheduleOnly` creates the
schedule without running containers, and `-Resume` continues an existing matrix
while skipping completed entries.

## Custom Matrix

The presets can be narrowed while debugging:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-full-experiment-matrix.ps1 `
  -MatrixName burst-check `
  -Scenarios burst `
  -Approaches static,adaptive `
  -Replications 2 `
  -StaticConsumerCounts 1,2
```

Valid approaches are `static`, `reactive`, and `adaptive`. Valid
scenario names correspond to `.properties` files in the selected scenario
directory. Smoke defaults to `producer/scenarios`; Dissertation defaults to
`experiments/scenarios`. Use `-ScenarioDirectory` for a custom location.

## Output

Results are stored below `results/<matrix-name>/`:

```text
results/<matrix-name>/
  matrix-manifest.json
  steady/
    static-c1-r1/
    reactive-r1/
    adaptive-r1/
  ...
```

The manifest records scenario, approach, replication, acknowledged and processed
events, failures, producer rate, peak/final lag, maximum consumer count, and the
path to each detailed summary. A run is marked `evidenceReady` only when its
process completed, every acknowledged event was processed, no processing failure
occurred, and final lag is zero. The top-level flag is true only when every run
passes those checks. Keep the entire result directory because Step 10 will use
the detailed latency and resource samples as well as the manifest.

Every run uses a matrix-, scenario-, approach-, and replication-specific Kafka
consumer group. This prevents committed offsets or backlog from an earlier run
from contaminating a later measurement.

The smoke workflow was verified on 2026-09-16. The corrected isolated static
validation acknowledged and processed 234 events, reported no failures, ended at
zero lag, and produced a top-level `evidenceReady` value of `true`.

Docker execution parity was verified in `docker-parity-smoke`: static, reactive,
and adaptive all used 1 CPU and 512 MiB consumer containers, all three runs were
evidence-ready, and the resulting matrix passed the Step 10 analyzer.

Randomization and resume were verified in `randomized-resume-smoke`. Seed
`20260917` produced the order adaptive, static, reactive; all three runs passed
the evidence gate. A subsequent resume skipped every completed entry without
creating an attempt `a2`.

## Experimental Discipline

- Use the same Docker CPU and memory limits for every approach.
- Record the Git commit, Docker Desktop resource allocation, Java version, and
  machine power mode before the main run.
- Run all replications in one matrix invocation where possible.
- Treat smoke output as functional verification, not dissertation evidence.
- Do not overwrite or manually edit result files.
