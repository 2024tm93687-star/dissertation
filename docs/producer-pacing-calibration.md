# Producer Pacing Calibration

## Diagnosis

The initial scheduler used `next = actualWakeTime + interval`. Any late wake-up
therefore shifted every subsequent deadline. A deterministic regression test
simulating a 10 ms oversleep at a target of 20 events/second reproduced the bug:
201 starts took 12.00 seconds instead of 10.01 seconds (10 seconds of scheduled
intervals plus one final 10 ms oversleep).

A fresh pre-fix live run on 2026-09-16 reproduced the shortfall: 201 events with
256-byte payloads at a target of 20 events/second took 12.699 seconds through
final acknowledgement, or 15.828 acknowledged events/second. All 201 events
were acknowledged. Run ID: `3483c149-336d-48e7-bbef-5fbcfe145839`.

The code and deterministic test establish accumulation of lateness; they do
not prove which operating-system mechanism caused individual wake-up delays.

## Correction

Advance the previous deadline by one interval when lateness is less than one
interval. On lateness of an entire interval or more, start a new schedule from
the current time. This corrects small cumulative drift while discarding missed
schedule slots after substantial stalls. Event count remains fixed, so a stalled
run takes longer instead of dropping messages.

Small corrections can shorten individual gaps; the rate is an average scheduling
target, not a strict per-gap maximum. The implementation retains blocking waits,
rechecks the clock on early wake-ups, and preserves interruption behavior.
It does not use spinning or change machine-wide timer settings.

The producer summary now separates event-start rate from acknowledgement rate
and reports schedule resets and maximum lateness. A single-event run has zero
event-start span and a `null` event-start rate.

## Verification

- The new deterministic oversleep regression failed on the old implementation and passed after the correction.
- Additional tests cover ordinary processing time, exact reset boundary, long stalls, clock wraparound, early wake-ups, and interruption during waiting.
- `mvn -B -ntp -f producer/pom.xml verify -Pintegration` passed 18 unit tests and 1 live Kafka integration test.
- The live integration test again checked count, payloads, IDs, keys, timestamps, and pacing, and removed its temporary topic.

## Live Results

Measured on 2026-09-16 with 201 events per run, 256-byte payloads, a fresh JVM
for each run, and the existing local Kafka resource limits:

| Version | Target events/s | Repetition | Event-start rate/s | Acknowledged rate/s | Resets |
| --- | --- | --- | --- | --- | --- |
| Before | 20 | 1 | Not recorded | 15.828 | Not recorded |
| After | 20 | 1 | 19.651 | 19.706 | 1 |
| After | 20 | 2 | 19.512 | 19.564 | 1 |
| After | 50 | 1 | 46.677 | 46.641 | 1 |
| After | 50 | 2 | 45.007 | 45.027 | 7 |

All events were acknowledged. The directly comparable acknowledgement metric
improved from 15.828 to 19.564-19.706 events/second at a target of 20. Event-start
errors at that target were -1.75% and -2.44%. At 50, errors were -6.65% and
-9.99%, so this short calibration does not establish reliable higher-rate timing.
Maximum lateness across these runs ranged from 165 to 271 ms. Reset counts make
those delays visible rather than concealing them with catch-up traffic.

Post-fix run IDs, in table order:

- `7c6af55f-60a5-4d4e-998e-c4bf74ddd5af`
- `eea42f61-ae27-4869-bc4a-68571f4b37d6`
- `1c5ff138-be25-453a-bb9a-a7b3142306ec`
- `919b6432-444d-4eee-b8da-327cb945d4f3`

Raw generated summaries are in
`producer/target/pacing-calibration-20260916-083605-392.json` (build output,
removed by `mvn clean`). This document preserves the main results. The calibration
script saves a new timestamped file on each invocation. The baseline and four
post-fix calibration runs added 1,005 records to `telecom-events`.

These are small functional calibration runs, with no warm-up, no isolated host,
and only two post-fix repetitions per rate. They are not dissertation benchmark
results. Before measured experiments, characterize longer runs and higher rates,
define warm-up, and separately measure actual broker arrival rate. See
[producer setup](producer-setup.md) for repeatable commands and metric definitions.

## Scope

This historical milestone corrected producer timing and added calibration
diagnostics. [Changing workload patterns and duration-based scenarios](producer-workload-patterns.md)
have since been implemented. This calibration does not establish high-rate
capacity or replace experiment-time arrival-rate metrics.

Reference: [Java 21 LockSupport documentation](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/locks/LockSupport.html)
describes the required recheck after a park returns, including early returns and
interruption.
