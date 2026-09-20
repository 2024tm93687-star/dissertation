# Producer Workload Patterns

## Run A Scenario

From the repository root, with Kafka running and `telecom-events` initialized:

```powershell
mvn -B -ntp -f producer/pom.xml package
java -jar producer/target/telecom-producer-0.1.0-SNAPSHOT.jar --spring.config.additional-location=file:producer/scenarios/burst-quiet.properties --debug=false
```

The process runs a 12-second workload, waits for outstanding sends, prints a
`PRODUCER_SUMMARY` JSON line, and exits. Each run has a fresh run ID and adds
synthetic records to `telecom-events`. A duration is measured after Kafka metadata
lookup; application startup and the final acknowledgement drain are additional.

Four ready-to-run files are available in `producer/scenarios/`:

| File | Timeline | Target rate, events/second |
| --- | --- | --- |
| `steady.properties` | 0-12 seconds | 20 |
| `ramp.properties` | 0-12 seconds | Linear increase from 10 to 40 |
| `burst.properties` | 0-3 / 3-6 / 6-12 seconds | 10 / 40 / 10 |
| `burst-quiet.properties` | 0-3 / 3-6 / 6-9 / 9-12 seconds | 10 / 40 / 0 / 10 |

Select a different filename in the command. CLI options override the file, so
`--workload.payload-size-bytes=1024` changes the payload size without editing it.

## Configuration Rules

Existing count-based steady commands still work. Without either limit, the
default remains 1,000 events. Set **either** `workload.event-count` **or**
`workload.duration`; setting both is a validation error. Duration-based runs
have no hidden 1,000-event cap, and their actual count depends on achieved rate.

| Property | Meaning |
| --- | --- |
| `workload.pattern` | `steady` (default), `ramp`, or `burst` |
| `workload.duration` | Positive duration up to one hour, e.g. `12s`, `500ms`, `2m` |
| `workload.rate-per-second` | Steady rate, ramp starting rate, or burst normal/recovery rate |
| `workload.peak-rate-per-second` | Required for ramp/burst; at least the base rate, at most 100,000 |
| `workload.burst-start` | Required for burst; nonnegative offset from workload start |
| `workload.burst-duration` | Required for burst; positive length of the high-rate phase |
| `workload.quiet-duration` | Optional silence immediately after burst; default zero |

Base rates are positive (1-100,000). Zero traffic is represented by a quiet
phase. Burst start + burst duration + quiet duration must fit within total
duration. Any remaining time is recovery at the base rate. Zero-length normal,
quiet, or recovery phases are omitted. A burst may start at zero or end at the
deadline. Phase-specific options on another pattern are rejected, as are unknown
options, negative durations, and ramp peaks below the base rate.

## Timing Semantics

Phase intervals include their start and exclude their end. The schedule uses
monotonic elapsed time, not message count or Kafka acknowledgement time.

For ramp, the target rate is `base + (peak - base) * elapsed / duration`.
The scheduler samples this rate at each event start to choose the next interval;
this is a discrete approximation to a linear rate curve, not an exact integral
event schedule. The peak is the endpoint target at the excluded deadline.

Short wake-up delays are corrected using the next scheduled deadline. Lateness
of a full interval resets scheduling instead of replaying overdue events. A stall
can skip an entire phase: it remains in the report with its full target and zero
observed events. Intentional phase transitions do not count as pacing resets;
the phase/window reports still reveal shortfalls across such transitions.

At the duration deadline, no new event generation is admitted. Work already
being generated, serialized, sent, or acknowledged can finish later. Similarly,
a quiet interval means no new event starts, not necessarily no broker arrivals:
previously buffered records can still arrive. Final acknowledgements have a
35-second drain timeout, separate from configured Kafka send timeouts.

OS scheduling, producer processing, and broker backpressure can lower achieved
rates. Count runs extend until every requested event is sent; duration runs do
not extend their generation timeline to make up missed traffic. Short duration
runs can produce zero events under extreme startup scheduling delays.

## Event And Summary Fields

Each event now includes `workloadPhase` (`steady`, `ramp`, `normal`, `burst`, or
`recovery`) and `workloadElapsedNanos`, the monotonic offset sampled immediately
before event generation. Quiet phases produce no events. Existing payload fields
remain unchanged; JSON metadata size grows by these two fields. Earlier records
already in Kafka lack these fields, so future consumers should tolerate their
absence or isolate experimental runs.

The final summary adds:

- `sent`: number accepted by the Kafka send API during the run.
- `requestedDurationSeconds`: duration limit, or `null` for count-based runs.
- `requested`: count limit, or `null` for duration-based runs.
- `phases`: target and observed counts/rates for every nonempty scheduled phase.
- `rateWindows`: the same measurements for one-second windows, with a shorter final window if needed.

Each phase/window has `startSeconds`, `endSeconds`, `targetAverageRatePerSecond`,
`sent`, `acknowledged`, `observedRatePerSecond`, and `rateErrorPercent`.
Observed rate is sent count divided by the **whole scheduled interval**. Target
rate is the integrated target curve divided by that interval, including quiet
time and partial phases inside a window. Signed error is
`100 * (observed - target) / target`; it is `null` for a zero target.

Both sent and acknowledged counts are attributed to the record's **generation
start** phase/window, even when the acknowledgement occurs later. They do not
measure broker arrival-time or acknowledgement-time rates. All sends must be
acknowledged before a successful summary is printed.

For example, a full quiet window reports target 0, sent 0, acknowledged 0,
observed 0, and error `null`. A missed active phase reports observed 0 and error
-100%. The global `eventStartRatePerSecond` spans first to last event, so it can
exclude trailing quiet time; use the window/phase rates for changing workloads.

Window storage is bounded by the one-hour limit (at most 3,600 windows), not by
the number of events. Reports are printed at the end; live metrics export remains
part of the later metrics-collection milestone.

## Verification

```powershell
mvn -B -ntp -f producer/pom.xml verify -Pintegration
```

Deterministic fake-clock tests exercise ramp rates, burst boundaries, quiet
intervals, deadline cutoff, skipped phases, fractional-window target integration,
oversleep handling, and interruption. Producer tests also hold an acknowledgement
past the duration deadline to verify backpressure does not admit another event.

Four live Kafka cases exercise the original count-based mode plus timed steady,
ramp, and burst/quiet/recovery. Each creates and deletes its own topic, consumes
all records, verifies payloads and IDs, checks event phase/deadline membership,
and reconciles every reported phase/window count with consumed records. These
tests check correctness; they do not require a loaded laptop to meet a precise
rate or imply benchmark-grade workload calibration.

## Verified Results

On 2026-09-16, all 30 unit tests and 4 live Kafka cases passed with
`mvn -B -ntp -f producer/pom.xml verify -Pintegration`.
The live burst case reconciled 20 normal, 40 burst, zero quiet, and 20 recovery
events with the records read from Kafka. All temporary test topics were removed.

The packaged application also ran the example files from the repository root:

| Scenario | Phase | Target events/s | Observed starts/s | Sent / acknowledged |
| --- | --- | --- | --- | --- |
| 12-second ramp | Ramp | 25 average (10 to 40) | 25.00 | 300 / 300 |
| 12-second burst/quiet | Normal, 0-3 s | 10 | 9.67 | 29 / 29 |
| 12-second burst/quiet | Burst, 3-6 s | 40 | 40.00 | 120 / 120 |
| 12-second burst/quiet | Quiet, 6-9 s | 0 | 0.00 | 0 / 0 |
| 12-second burst/quiet | Recovery, 9-12 s | 10 | 10.00 | 30 / 30 |

Ramp run ID: `74a35c9c-c74e-4115-9768-2ae2128790e8`.
Burst/quiet run ID: `e30c5266-bdc0-426e-b808-e474448cf3f4`.
The initial normal phase's -3.33% error is retained in its summary. These short
runs do not establish accuracy at higher rates or statistical repeatability.

Raw summaries, including all one-second windows, are saved in
`producer/target/workload-pattern-verification.json`. This generated artifact is
removed by `mvn clean`; the table above preserves the main observations. The
479 packaged-example records remain in `telecom-events` subject to retention.

Mixed payload sizes, long-run calibration, and experiment automation are still
future work. The next application component is the configurable consumer.
