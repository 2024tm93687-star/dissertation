package org.dissertation.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

final class TimelineMetrics {
    private static final long SECOND = 1_000_000_000L;
    private final List<Counter> phases = new ArrayList<>();
    private final List<Counter> windows = new ArrayList<>();
    private final WorkloadTimeline timeline;

    TimelineMetrics(WorkloadTimeline timeline) {
        this.timeline = timeline;
        for (var phase : timeline.phases()) {
            phases.add(new Counter(phase.name(), phase.start(), phase.end()));
        }
        for (long start = 0; start < timeline.durationNanos(); start += SECOND) {
            windows.add(new Counter("second-" + start / SECOND, start,
                    Math.min(start + SECOND, timeline.durationNanos())));
        }
    }

    Runnable recordSend(long elapsed) {
        Counter phase = phases.get(timeline.phaseIndexAt(elapsed));
        Counter window = windows.get((int) (elapsed / SECOND));
        phase.sent++;
        window.sent++;
        return () -> {
            phase.acknowledged.incrementAndGet();
            window.acknowledged.incrementAndGet();
        };
    }

    List<RateObservation> phases() { return phases.stream().map(this::observe).toList(); }
    List<RateObservation> windows() { return windows.stream().map(this::observe).toList(); }

    private RateObservation observe(Counter counter) {
        double seconds = (counter.end - counter.start) / 1e9;
        double target = timeline.expectedEvents(counter.start, counter.end) / seconds;
        double actual = counter.sent / seconds;
        return new RateObservation(counter.name, counter.start / 1e9, counter.end / 1e9, target,
                counter.sent, counter.acknowledged.get(), actual,
                target == 0 ? null : 100 * (actual - target) / target);
    }

    private static final class Counter {
        private final String name;
        private final long start;
        private final long end;
        private long sent;
        private final AtomicLong acknowledged = new AtomicLong();

        private Counter(String name, long start, long end) {
            this.name = name;
            this.start = start;
            this.end = end;
        }
    }
}
