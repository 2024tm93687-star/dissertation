package org.dissertation.producer;

import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

final class TimelinePacer {
    private final WorkloadTimeline timeline;
    private final long origin;
    private final LongSupplier clock;
    private final LongConsumer pause;
    private long next;
    private int previousPhase = -1;
    private long resets;
    private long maxLateness;

    TimelinePacer(WorkloadTimeline timeline, long origin) {
        this(timeline, origin, System::nanoTime, LockSupport::parkNanos);
    }

    TimelinePacer(WorkloadTimeline timeline, long origin, LongSupplier clock, LongConsumer pause) {
        this.timeline = timeline;
        this.origin = origin;
        this.clock = clock;
        this.pause = pause;
    }

    // Returns elapsed monotonic time, or -1 after the workload deadline.
    long awaitSlot() throws InterruptedException {
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Workload interrupted");
            }
            long elapsed = clock.getAsLong() - origin;
            if (elapsed >= timeline.durationNanos()) {
                return -1;
            }
            int index = timeline.phaseIndexAt(elapsed);
            var phase = timeline.phases().get(index);
            if (index != previousPhase) {
                next = elapsed;
                previousPhase = index;
            }
            double rate = phase.rateAt(elapsed);
            if (rate == 0) {
                pause.accept(phase.end() - elapsed);
                continue;
            }
            if (elapsed < next) {
                pause.accept(Math.min(next, phase.end()) - elapsed);
                continue;
            }
            long interval = (long) Math.ceil(1e9 / rate);
            long lateness = elapsed - next;
            maxLateness = Math.max(maxLateness, lateness);
            if (lateness >= interval) {
                next = elapsed + interval;
                resets++;
            } else {
                next += interval;
            }
            next = Math.min(next, phase.end());
            return elapsed;
        }
    }

    long resetCount() { return resets; }
    long maxLatenessNanos() { return maxLateness; }
}
