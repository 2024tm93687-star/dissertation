# Results Analysis

Step 10 converts a Step 9 matrix into reproducible tables and charts using only
PowerShell. No Python, R, or plotting package is required.

## Run the Analyzer

Analyze only runs that passed the matrix evidence-quality gate:

```powershell
powershell -ExecutionPolicy Bypass -File analysis/analyze-experiment-matrix.ps1 `
  -MatrixManifest results/dissertation-main/matrix-manifest.json `
  -CpuCoresPerConsumer 1
```

Generated files are stored in `results/dissertation-main/analysis/`:

- `run-metrics.csv`: detailed row for every included run.
- `aggregate-metrics.csv`: means and sample standard deviations by scenario and approach.
- `analysis-report.md`: dissertation-friendly summary table and metric definitions.
- `throughput-per-core.svg`: resource-normalized throughput chart.
- `latency-p95.svg`: end-to-end p95 latency chart.
- `peak-lag.svg`: peak consumer-lag chart.
- `analysis-summary.json`: machine-readable analysis metadata.

For diagnostics only, `-IncludeInvalidRuns` includes runs that failed the
evidence-quality gate. Such output must not be used as dissertation evidence.

## Metric Interpretation

The analyzer uses acknowledged producer rate as achieved throughput. Since a
global latency histogram is not currently stored, it reports the maximum of the
per-consumer p95 latency values rather than averaging percentiles. This is a
conservative and statistically valid description of the captured summaries.

Average active consumer count comes from the controller's lag samples. Static
runs use their fixed count. Normalized throughput is:

```text
throughput_per_cpu_core =
    achieved_throughput / (average_active_consumers * cpu_cores_per_consumer)
```

Scaling efficiency compares throughput per active consumer with the matching
static one-consumer run from the same scenario and replication:

```text
scaling_efficiency =
    approach_throughput_per_consumer / static_one_consumer_throughput
```

A value near 1.0 indicates close-to-linear resource efficiency. Lower values
show overhead or idle capacity. The CPU-core measure is only defensible when the
configured Docker CPU allocation matches `-CpuCoresPerConsumer`; record that
allocation in the dissertation methodology.

## Statistical Use

Use the dissertation preset's three replications for final comparisons. Report
means together with sample standard deviations, retain individual run rows, and
discuss practical effect sizes rather than relying only on the best run. Smoke
matrices validate tooling and must not be presented as research findings.

The analyzer was smoke-verified on 2026-09-16 in both default evidence-only mode
and diagnostic `-IncludeInvalidRuns` mode. Both CSV tables, the Markdown report,
analysis metadata, and all three SVG charts were generated successfully.
