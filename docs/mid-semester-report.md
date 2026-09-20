# Mid-Semester Dissertation Progress Report

## Adaptive Workload-Aware Consumer Scaling and Configuration for Kafka Consumers

**Programme:** Master of Technology (M.Tech.) in Software Engineering  
**Submitted by:** [Student Name]  
**Registration/Roll Number:** [Registration Number]  
**Supervisor:** [Supervisor Name and Designation]  
**Department:** [Department Name]  
**Institution/University:** [Institution/University Name]  
**Academic Session:** [Academic Session]  
**Semester:** [Semester]  
**Report Date:** 19 September 2026

---

## Abstract

This dissertation investigates workload-aware scaling and configuration of Apache Kafka consumers under changing synthetic telecom traffic. The central research question is whether a controller can select sufficient consumer capacity and suitable consumer settings while controlling latency and lag and limiting unnecessary resource allocation. The proposed evaluation compares a fixed-capacity baseline, a reactive lag-threshold baseline, and an adaptive workload-aware approach within a controlled Docker environment.

At the mid-semester stage, a working experimental prototype has been developed. It includes Spring Boot producer and consumer applications, configurable processing cost, Docker-based consumer execution, structured metric collection, Micrometer instrumentation, Prometheus and Grafana configuration, and an automated experiment and analysis workflow. The current Docker adaptive prototype selects configuration according to payload size, plans initial capacity from configured workload information, and subsequently responds to consumer lag. A separate Spring Boot controller with live workload estimation remains to be implemented.

Preliminary verification demonstrates successful event production and consumption, container lifecycle control, isolated experiment groups, deterministic schedule generation, and skipping completed experiments during resume. A randomized three-approach smoke matrix completed with matching acknowledged and processed counts and zero final lag in every run. A 60-run evaluation design has been recorded using four workload scenarios, three replications, three fixed consumer counts, and two dynamic policies. These preliminary results establish functional progress; they do not yet demonstrate a performance advantage of the adaptive approach. The remaining work includes completing the controller feedback loop, strengthening measurement and recovery behavior, calibrating workloads, collecting the final dataset, and evaluating the latency-resource trade-offs.

**Keywords:** Apache Kafka, consumer scaling, adaptive control, workload awareness, Docker, Spring Boot, telecom workloads, performance evaluation.

## Contents

1. Introduction and Motivation
2. Problem Statement and Research Objectives
3. Background and Preliminary Literature Study
4. Scope and Research Methodology
5. System Architecture and Design
6. Implementation Progress
7. Experimental Design and Measurements
8. Preliminary Verification Results
9. Challenges, Limitations, and Threats to Validity
10. Remaining Work and Schedule
11. Expected Contributions and Conclusion
12. References
13. Appendix: Repository Evidence

## 1. Introduction and Motivation

Event-driven applications must handle traffic whose volume and processing requirements vary over time. Telecom-style event streams provide a useful experimental setting because they can be represented by steady traffic, gradual increases, bursts, and quiet intervals. The project uses synthetic telecom-style events to study these changes in a reproducible environment; it does not claim to reproduce an operational telecom network.

Kafka separates event publication from event consumption. This allows consumers to process retained events at their own pace, but a sustained difference between arrival rate and processing capacity creates a backlog. Allocating additional consumers can help where partition parallelism and available resources permit. However, excessive allocation increases resource consumption, and changes in group membership introduce coordination overhead.

The dissertation therefore considers capacity selection and consumer configuration together. The number of consumers determines available parallelism, while polling and fetching settings influence how records reach the application. A useful controller should respond to workload changes without treating every increase in backlog as evidence that another consumer is necessarily beneficial.

From a software engineering perspective, the work combines requirements analysis, modular implementation, observability, automated testing, experimental reproducibility, and evidence-based evaluation. The personal-computer environment makes implementation accessible while requiring careful limits on the conclusions drawn from performance results.

## 2. Problem Statement and Research Objectives

### 2.1 Problem Statement

A fixed consumer count can be insufficient during high demand and unnecessarily large during low demand. A simple reactive policy can respond to observed lag, but lag alone does not describe payload size, processing cost, resource pressure, or the likely benefit of scaling. Consumer configuration can also affect performance independently of consumer count.

The problem is to design and evaluate a controller that uses workload and performance information to make capacity and configuration decisions within explicit resource and partition limits.

### 2.2 Main Research Question

> For a changing workload, can the system automatically decide the required Kafka consumer capacity and consumer configuration while keeping latency and consumer lag under control and avoiding unnecessary resource usage?

### 2.3 Objectives

1. Build a reproducible Kafka test environment with controlled broker and consumer resources.
2. Generate synthetic workloads with configurable rate, payload size, and temporal pattern.
3. Implement consumers with configurable Kafka properties and measurable processing behavior.
4. Establish static and reactive comparison baselines.
5. Develop an adaptive policy using workload information and measured performance.
6. Automate experiment execution, result collection, and reproducible analysis.
7. Evaluate latency, backlog, scaling behavior, and resource-normalized performance under matched conditions.

### 2.4 Evaluation Questions

The final study will examine whether the adaptive policy reduces backlog or latency during workload changes, whether it avoids unnecessary capacity during low demand, and whether configuration selection adds measurable value beyond scaling alone. These are questions to be tested, rather than conclusions established by the current prototype.

## 3. Background and Preliminary Literature Study

### 3.1 Kafka and Stream Consumption

The original Kafka paper describes a distributed messaging system intended to collect and deliver large volumes of log data. It provides background for the dissertation's use of a durable event stream and independent producers and consumers. The present work builds at the application and orchestration layers rather than modifying Kafka's storage implementation. See Kreps, Narkhede, and Rao, [Kafka: a Distributed Messaging System for Log Processing](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/09/Kafka.pdf).

### 3.2 Consumer Configuration

Kafka exposes settings for controlling polling and fetching. In particular, `max.poll.records` limits the records returned by a poll; it does not directly limit the underlying fetch size. Fetch-related properties must therefore be considered separately. This distinction motivates explicit recording of both polling and fetching settings in the experiments. See the [Apache Kafka consumer configuration reference](https://kafka.apache.org/42/configuration/consumer-configs/).

### 3.3 Observability

Micrometer provides a Prometheus registry through which instrumented measurements can be exposed for scraping. This supports the project's monitoring design: application instrumentation supplies measurements, Prometheus collects them, and Grafana presents them. The current experimental evidence also includes structured files that preserve run-specific results. See the [Micrometer Prometheus integration documentation](https://docs.micrometer.io/micrometer/reference/implementations/prometheus.html).

### 3.4 Position of the Proposed Work

The proposed contribution is an empirical evaluation of combined consumer-capacity and configuration decisions under matched local resource limits. Static allocation, reactive scaling, and workload-informed decisions provide the three comparison approaches. A broader review of published Kafka autoscaling, stream-processing elasticity, and adaptive configuration research is still required to establish the precise research gap. At this stage, originality is a research objective, not a demonstrated claim of priority over existing systems.

## 4. Scope and Research Methodology

The work follows a prototype-and-evaluate methodology. Requirements are translated into independently configurable components, each component is verified, and integrated experiments are used to assess the behavior of the complete system.

The current scope comprises one local Kafka broker, a six-partition topic, a synthetic producer, and containerized consumers. The dynamic policies are bounded between one and four consumers. Experiments vary event rate, payload size, and workload pattern. Consumer processing uses configurable computation and delay to provide a controlled processing workload.

The study does not currently include broker-cluster scaling, production telecom records, multi-region deployment, or production capacity certification. Network conditions, storage behavior, and resource interference on a laptop differ from those in a dedicated hosting environment. Normalized measures will be used to describe observed trends without assuming direct production extrapolation.

The research sequence is requirements definition, prototype construction, functional verification, workload calibration, controlled comparison, statistical description, and interpretation of threats to validity. Development smoke tests and final measured experiments have distinct purposes and will be reported separately.

## 5. System Architecture and Design

### 5.1 Implemented Prototype

```text
             Experiment matrix and policy runner (PowerShell)
                  | workload launch       | Docker lifecycle
                  v                       v
         Spring Boot producer       Consumer containers
                  |                       ^
                  v                       |
             Kafka broker ------------ topic partitions
                  |
                  +--> Kafka group/offset information --> runner

       Producer summaries + consumer metrics + lag/scale logs
                               |
                               v
                       Result JSON/JSONL files
                               |
                               v
                Analysis script --> CSV, report, SVG charts

       Optional live observation:
       Micrometer/Actuator and Kafka exporter --> Prometheus --> Grafana
```

The matrix runner controls experimental order and policy selection. The shared Docker runner starts and stops consumers and records their results. Policy mode is configurable, so the comparison can be executed without editing Java source between runs.

### 5.2 Proposed Controller Service

The accepted design includes a separate Spring Boot controller that observes workload and consumer behavior, obtains Kafka information using an administrative client, and applies scaling and configuration decisions through Docker. This service is a remaining deliverable. The current orchestration is implemented in PowerShell, and the shared Docker runner obtains lag through Kafka command-line tooling.

### 5.3 Separation of Responsibilities

The producer owns workload generation. The consumer owns processing and application measurements. Kafka provides the event stream and group coordination. The policy runner owns capacity decisions and container lifecycle. The experiment runner owns schedule and result organization. The analyzer owns aggregation and reporting. This division supports isolated testing and makes experimental assumptions easier to inspect.

## 6. Implementation Progress

| Component | Mid-semester progress | Evidence or boundary |
|---|---|---|
| Kafka environment | Implemented and smoke-tested | Docker Compose, six-partition topic, configured broker limits |
| Synthetic producer | Implemented | Count/duration workloads, steady/ramp/burst patterns, seed and pacing reports |
| Configurable consumer | Implemented | Processing cost, poll/fetch options, metric snapshots and offset handling |
| Static baseline | Implemented | Fixed consumer count through the common Docker backend |
| Reactive baseline | Implemented | Lag-threshold scaling with cooldown and post-producer scale-down |
| Docker adaptive prototype | Implemented in simplified form | Initial workload-based capacity, payload-based settings, subsequent lag response |
| Full adaptive controller | Partially realized across prototypes | Spring Boot service and complete live feedback integration remain pending |
| Monitoring | Instrumentation and stack configuration implemented | Dynamic consumer scraping needs further integration |
| Experiment orchestration | Implemented and smoke-tested | Randomized schedule, checkpoints, attempt-specific groups and directories |
| Analysis | Implemented and smoke-tested | Per-run and aggregate CSV, Markdown report and three SVG charts |
| Final comparison | Planned | Full 60-run measured dataset and conclusions remain pending |

### 6.1 Workload Generation and Consumption

The producer accepts configurable event count or duration, target rate, payload size, seed, and workload pattern. Its summaries record acknowledged counts and observed rate behavior. Phase and time-window reports make deviations from the requested workload visible.

The consumer supports configurable polling and fetching settings, processing iterations, and processing delay. It reports processed records, failures, bytes, processing time, end-to-end latency, and partition-assignment information. The implementation is intended for at-least-once processing; equality of aggregate counts during smoke tests does not establish exactly-once delivery.

### 6.2 Current Policy Behavior

The static policy holds the configured consumer count constant. The Docker reactive policy scales up when lag exceeds its threshold, subject to the maximum count and cooldown. Its scale-down path currently applies after the producer exits and lag has drained.

The Docker adaptive prototype reads expected rate and payload size from experiment configuration. For a changing-rate scenario, it uses the configured peak rate in initial planning. Its initial consumer estimate is:

```text
initial_consumers = ceil(expected_rate * headroom / initial_capacity_per_consumer)
```

The result is bounded by the configured minimum and maximum. The initial capacity value is an assumption that needs calibration. The current Docker path does not continuously learn processing capacity or discover the changing arrival rate from live ingress measurements.

Payload size selects the startup configuration as follows:

| Payload size | max.poll.records | max.partition.fetch.bytes |
|---|---:|---:|
| Up to 512 bytes | 500 | 1,048,576 |
| 513 to 4,096 bytes | 200 | 4,194,304 |
| Above 4,096 bytes | 50 | 8,388,608 |

In these profiles, `fetch.min.bytes` remains 1. Subsequent Docker adaptive scaling is driven by lag pressure. Although a latency target is recorded, it is not currently used in this Docker decision loop. The earlier host-JVM adaptive script contains additional capacity, lag-trend, latency, and broker-resource logic; that richer feedback has not yet been fully transferred into the common Docker path. Runtime reconfiguration of already-running consumers is also pending.

### 6.3 Monitoring and Automation

Micrometer and Actuator dependencies are present in the consumer. Prometheus and Grafana configuration and a Kafka exporter are available. The current Prometheus consumer target is a configured host endpoint; automatic discovery and scraping of every dynamically created consumer container are not yet established. Structured result files remain the primary evidence for the present experiment workflow.

The matrix runner generates the full schedule before execution, shuffles it with a recorded seed, and saves per-entry status. Completed-run skipping has been demonstrated. Failure and interruption handling require additional tests before relying on unattended execution of the final dataset.

## 7. Experimental Design and Measurements

### 7.1 Controlled Environment

| Parameter | Recorded configuration |
|---|---|
| Host platform | Windows development machine with Docker |
| Application implementation | Java 21 and Spring Boot |
| Broker count | 1 |
| Topic | telecom-events |
| Partitions | 6 |
| Broker resource limits | 2 CPU cores and 2 GiB memory |
| Consumer resource limits | 1 CPU core and 512 MiB memory per container |
| Fixed consumer counts | 1, 2, 4 |
| Dynamic consumer bounds | 1 to 4 |
| Simulated processing | 1,000 iterations and 5 ms delay |
| Requested sampling interval | 2 seconds |
| Matrix cooldown | 8 seconds |
| Schedule seed | 20260917 |

The sampling interval is a requested delay between iterations. The duration of metric collection and Docker commands contributes to the actual interval and must be measured.

### 7.2 Planned Workloads

| Scenario | Duration | Rate pattern | Payload |
|---|---:|---|---:|
| steady-small | 60 seconds | 30 events/s | 256 bytes |
| steady-large | 60 seconds | 30 events/s | 8,192 bytes |
| ramp-small | 60 seconds | Linear increase from 20 to 120 events/s | 256 bytes |
| burst-quiet-medium | 60 seconds | 20 events/s for 15 s; 120 for 20 s; quiet for 10 s; 20 for 15 s | 2,048 bytes |

Payload sizes refer to the synthetic payload field; serialized event size also includes metadata. Three replications are planned for each condition. The total is:

```text
4 scenarios * 3 replications * (3 static sizes + 1 reactive + 1 adaptive) = 60 runs
```

The repository labels protocol version 1.0 as frozen. In this report, that label denotes the recorded experimental snapshot. It does not establish that the full proposed controller or all measurement requirements are complete. Any changes arising from the remaining methodology review should receive a new protocol version before final collection.

### 7.3 Measurements and Analysis

The recorded measurements include producer acknowledgements, consumer processed and failed counts, end-to-end latency, processing time, lag samples, container lifecycle events, and consumer counts. CPU and memory limits are controlled; this does not mean that measured CPU and memory utilization is available for every dynamic consumer across the complete run.

The current analyzer reports producer acknowledgement rate as throughput. This is useful as a measure of delivered producer load, but it is not a direct measure of consumer saturation capacity or backlog-drain throughput. Consumer throughput over a common measurement window is required for the final capacity comparison.

Existing normalized calculations are:

```text
throughput_per_assumed_consumer_core =
    producer_acknowledgement_rate / (average_active_consumers * cores_per_consumer)

relative_per_consumer_efficiency =
    approach_throughput_per_consumer / matching_static_one_consumer_throughput
```

These are allocation-based proxies. The current average consumer count is an arithmetic mean of sampled values, which may be affected by irregular sampling. Time-weighted consumer-seconds and allocated core-seconds will provide a stronger basis for dynamic resource comparisons.

For latency, the analyzer reports the maximum available per-instance p95 value. This describes the largest reported consumer percentile, not a pooled group p95. A group percentile requires compatible merged histograms or event-level observations. The analyzer provides throughput mean and sample standard deviation across replications, with means for additional metrics. Broader uncertainty reporting remains part of the final analysis work.

## 8. Preliminary Verification Results

### 8.1 Recorded Component Checks

Development documentation records producer unit and live integration checks, consumer unit and live integration checks, and end-to-end smoke experiments. The producer workload milestone records 30 unit tests and four live Kafka cases; the initial consumer milestone records 17 unit tests and three live Kafka cases. These figures are historical milestone records and are not presented as a fresh test-suite execution for this report.

### 8.2 Randomized Three-Approach Smoke Run

The saved matrix `randomized-resume-smoke`, completed on 17 September 2026, used a 12-second steady workload with 256-byte payloads. The recorded execution order was adaptive, static, and reactive.

| Execution order | Approach | Acknowledged | Processed | Failures | Peak sampled lag | Final lag |
|---:|---|---:|---:|---:|---:|---:|
| 1 | Adaptive | 236 | 236 | 0 | 3 | 0 |
| 2 | Static, one consumer | 236 | 236 | 0 | 4 | 0 |
| 3 | Reactive | 232 | 232 | 0 | 3 | 0 |

All three entries passed the implemented `evidenceReady` gate. That gate checks completion, matching aggregate counts, zero failures, and zero final lag. The evidence is the saved matrix manifest, not a newly executed benchmark.

These results show that the three modes can execute and produce analyzable records. The small differences in sampled lag do not establish a ranking: there is one short run per approach, achieved event counts differ, and the workload does not characterize sustained saturation. Matching counts also do not prove event-level uniqueness or complete telemetry coverage.

### 8.3 Scaling and Scheduling Checks

The earlier `docker-parity-smoke` matrix recorded matching counts of 235, 234, and 236 for static, reactive, and adaptive respectively, with zero final lag. Its adaptive run recorded two container starts and subsequent scale-down. The second container produced no metric reports before it stopped; therefore, this verifies lifecycle actuation, not useful additional processing capacity.

Two saved dissertation schedules generated with seed `20260917` contained 60 unique entries in identical order. A resume invocation against the completed randomized smoke matrix skipped all entries and retained attempt number 1. This demonstrates completed-run skipping; it does not by itself validate recovery from a process crash, corrupt checkpoint, or a partially running workload.

## 9. Challenges, Limitations, and Threats to Validity

### 9.1 Engineering Lessons

Early static trials exposed two problems: the consumer's finite runtime could expire before workload completion, and reusing a group name could include earlier backlog in a later trial. Longer runtime during initial validation and experiment-specific consumer groups addressed the observed cases. The common Docker execution path subsequently removed finite host-JVM lifetime differences from the matrix's consumer lifecycle.

Containerizing all three matrix modes also addressed a major comparability issue: resource-limited Docker consumers and unconstrained host JVM consumers should not be treated as equivalent resource allocations. The current matrix uses the same image and per-container limits across policy modes.

### 9.2 Remaining Implementation Gaps

The complete adaptive feedback loop and the proposed Spring Boot controller remain the most significant gaps. Live incoming rate, observed processing capacity, latency, and consumer CPU/memory pressure need to inform the integrated decisions. Scale-down during a quiet period while the producer remains alive also needs implementation and evaluation. Configuration changes should be applied through controlled restarts and evaluated with their disruption cost.

The lag reader and readiness checks require strengthening. In the current loop, unavailable lag can be converted to zero for decision-making. Missing observations must be distinguished from a drained group. The readiness test should verify current assignment across all expected partitions before production starts. Docker stop duration must also be distinguished from actual partition-rebalance or processing-interruption duration.

### 9.3 Experimental Validity

Workload calibration is necessary because the requested high rate may differ from the achieved rate, and a 60-second run can be dominated by startup and coordination overhead. Warm-up, observation windows, sampling overhead, drain deadlines, and workload intensity should be selected using pilot measurements.

The current adaptive and reactive matrix modes also use different lag thresholds. An observed difference could therefore reflect threshold choice as well as workload awareness. Matched-threshold comparisons or ablation experiments are needed to isolate the effect of capacity planning and configuration selection.

Three replications provide an initial descriptive comparison but limited statistical precision. The small and large steady scenarios isolate payload size at the same rate; other scenarios change several factors together. Configuration benefits should be assessed with additional controlled comparisons where necessary.

Aggregate-count validation should be supplemented with event/run identifiers, fresh complete lag observations, and telemetry-completeness checks. Performance overload outcomes must be reported, not silently removed because lag did not drain. Infrastructure failures and legitimate policy performance failures require separate classification to avoid selection bias.

### 9.4 Reproducibility and Generalization

Resume support needs tests for failed-quality runs, interrupted children, leftover containers, and checkpoint interruption. Completed status currently does not alone guarantee that an entry passed the evidence gate. Checkpoint integrity, artifact hashes, executable/image versions, and preserved attempt histories require verification before final unattended collection.

Docker limits reduce variation but do not eliminate shared-host effects from storage, background applications, power management, or virtualization. Identical seeds reproduce workload settings and schedule generation, not identical operating-system timing. Results will be scoped to the measured environment and synthetic workloads. No production-capacity or general adaptive-superiority claim is made at this stage.

## 10. Remaining Work and Schedule

The following eight-week schedule is indicative and should be aligned with the institution's submission dates and supervisor feedback.

| Period | Planned work | Expected deliverable |
|---|---|---|
| Week 1 | Extend literature review; confirm contribution and requirements against the approved proposal | Related-work comparison and requirements traceability |
| Weeks 2-3 | Implement the Spring Boot controller and integrate live capacity, workload, latency and resource signals | Integrated adaptive service and focused policy tests |
| Week 4 | Complete telemetry, configuration restart behavior, readiness and recovery checks | Validated collection and lifecycle workflow |
| Week 5 | Calibrate workloads; define warm-up/windows; review thresholds and ablations; version the revised protocol | Pilot results and approved final experiment specification |
| Week 6 | Capture environment and execute controlled replicated experiments | Complete results with all failures and attempts retained |
| Week 7 | Calculate normalized metrics, uncertainty and effect sizes; examine alternative explanations | Tables, plots, and results discussion |
| Week 8 | Complete dissertation chapters, supervisor revisions, demonstration and presentation | Submission draft and demonstration package |

The immediate priority is to close the gap between the simplified Docker adaptive prototype and the accepted controller design before treating the 60-run matrix as final research evidence.

## 11. Expected Contributions and Conclusion

The expected dissertation contributions are a workload-aware consumer controller, a reproducible comparison with static and reactive baselines, and an analysis of capacity, configuration, latency, and resource trade-offs under controlled constraints. The experimental framework is already a substantial software engineering artifact: it supports configurable workloads, common container execution, traceable results, randomized schedules, and automated reporting.

At mid-semester, the project has established a working prototype and demonstrated its principal data path and orchestration workflow. Recorded smoke runs show consistent aggregate event accounting and successful completion. The central research question remains open. The next phase will complete the proposed adaptive service and measurement design, collect the controlled dataset, and determine whether workload awareness provides a measurable benefit under the tested conditions.

## 12. References

1. J. Kreps, N. Narkhede, and J. Rao, "Kafka: a Distributed Messaging System for Log Processing," NetDB Workshop, 2011. [Paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/09/Kafka.pdf).
2. Apache Software Foundation, "Consumer Configs," Apache Kafka 4.2 documentation. [Configuration reference](https://kafka.apache.org/42/configuration/consumer-configs/). Accessed 19 September 2026.
3. Micrometer, "Micrometer Prometheus," reference documentation. [Prometheus integration](https://docs.micrometer.io/micrometer/reference/implementations/prometheus.html). Accessed 19 September 2026.
4. Project repository, [Dissertation Implementation Plan](dissertation-implementation-plan.md), development progress record.
5. Project repository, [Final Experiment Protocol](final-experiment-protocol.md) and [protocol configuration](../experiments/protocols/dissertation-main-v1.json).
6. Project repository, [Producer Workload Patterns](producer-workload-patterns.md), [Consumer Setup](consumer-setup.md), and [Results Analysis](results-analysis.md), implementation and historical verification records.

## 13. Appendix: Repository Evidence

| Artifact | Location | Relevance |
|---|---|---|
| Kafka and monitoring configuration | docker/docker-compose.yml | Environment and resource settings |
| Producer | producer/ | Synthetic workload implementation |
| Consumer | consumer/ | Processing, configuration and instrumentation |
| Docker policy runner | experiments/run-docker-scaling.ps1 | Current three-mode container execution |
| Earlier adaptive script | experiments/run-adaptive-controller.ps1 | Host-JVM feedback prototype |
| Matrix runner | experiments/run-full-experiment-matrix.ps1 | Schedule, checkpoints and attempts |
| Workload specifications | experiments/scenarios/ | Four proposed measured scenarios |
| Environment capture | experiments/capture-experiment-environment.ps1 | Version and environment record |
| Analyzer | analysis/analyze-experiment-matrix.ps1 | CSV, Markdown and SVG generation |
| Randomized smoke evidence | results/randomized-resume-smoke/matrix-manifest.json | Three-mode preliminary results |
| Docker parity evidence | results/docker-parity-smoke/matrix-manifest.json | Common execution and lifecycle check |
| Schedule verification | results/schedule-validation-a/execution-schedule.json; results/schedule-validation-b/execution-schedule.json | Recorded deterministic 60-run schedules |

The `results/` directory is excluded from Git in the current repository. Supporting evidence should be archived separately with the report and final dissertation dataset. Student and institution placeholders on the title page must be completed before submission.
