# Final Experiment Protocol

## Protocol Status

Protocol file: `experiments/protocols/dissertation-main-v1.json`

Version 1 is **frozen**. Workload definitions, replication count, quality
criteria, broker limits, consumer resource targets, and randomization seed
`20260917` are fixed. Static, reactive, and adaptive matrix runs execute through
the same Docker-constrained consumer backend.

## Research Matrix

Each approach uses three replications of four 60-second scenarios:

| Scenario | Pattern | Rate (events/s) | Payload | Purpose |
|---|---|---:|---:|---|
| `steady-small` | Steady | 30 | 256 B | Low-load control |
| `steady-large` | Steady | 30 | 8 KiB | Payload/configuration sensitivity |
| `ramp-small` | Linear ramp | 20 to 120 | 256 B | Gradual capacity change |
| `burst-quiet-medium` | 20, then 120, quiet, recovery | 2 KiB | Burst response and scale-down |

Static runs use 1, 2, and 4 consumers. Reactive and adaptive runs use 1-4
consumers. This yields 60 runs: 36 static plus 12 reactive and 12 adaptive.

Kafka is fixed at six partitions, 2 CPU cores, and 2 GiB memory. Each consumer
target is 1 CPU core and 512 MiB memory. Consumer processing uses 1,000 CPU
iterations and 5 ms simulated delay. The controller samples every two seconds,
uses an eight-second cooldown, and targets p95 latency below 2,000 ms.

## Validity Gate

A run is accepted only when:

- The producer exits successfully.
- Processed records equal acknowledged records.
- Consumer processing failures equal zero.
- Final committed consumer lag equals zero.
- The matrix marks the run `evidenceReady=true`.

Failed runs are retained with logs, excluded from analysis, and repeated using a
new consumer group. Never delete a failed run and silently replace its files.

## Run Conditions

Before each experiment session:

1. Reboot or establish a documented stable machine state.
2. Connect AC power and use the same Windows power mode.
3. Close unrelated CPU-, memory-, disk-, and network-intensive applications.
4. Fix Docker Desktop CPU and memory allocation and record the values.
5. Keep Kafka topic partition count and Docker limits unchanged.
6. Build once before the measured sequence; do not rebuild between runs.
7. Capture the environment record and preserve it with the matrix.

Capture the machine and tool state with:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/capture-experiment-environment.ps1
```

The JSON record includes protocol and Compose hashes, Git state, Java/Maven/
Docker versions, host processor count, and running containers. The repository
should be committed and clean for the final benchmark. Power mode, Docker Desktop
global allocation, and background applications must be recorded manually because
they are not reliably exposed by the command-line tools used here.

## Execution Order

The matrix constructs all 60 runs before execution and applies a seeded
Fisher-Yates shuffle. `execution-schedule.json` is written before the first run
and records sequence, scenario, approach, static count, replication, attempts,
timestamps, status, errors, and result links. Seed `20260917` reproduced the same
60-entry order in two independent schedule-generation checks.

Every entry is checkpointed before and after execution. Resume an interrupted
matrix with the same name and seed:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-full-experiment-matrix.ps1 `
  -Preset Dissertation `
  -MatrixName dissertation-main `
  -RandomSeed 20260917 `
  -Resume
```

Completed entries are skipped. A failed or interrupted entry is retried with an
incremented attempt number and a fresh consumer group, while prior artifacts are
retained. Saved execution settings are reloaded during resume to prevent drift.

Update the protocol version and document the reason for every future change.
Once the first measured run starts, do not modify
code, scenarios, thresholds, limits, or analysis definitions within that dataset.
