package org.dissertation.producer;

import java.util.ArrayList;
import java.util.List;

final class WorkloadTimeline {
    record Phase(String name, long start, long end, double startRate, double endRate) {
        double rateAt(long elapsed) {
            return startRate + (endRate - startRate) * (elapsed - start) / (double) (end - start);
        }

        double expectedEvents(long from, long to) {
            long left = Math.max(start, from);
            long right = Math.min(end, to);
            return right <= left ? 0 : (rateAt(left) + rateAt(right)) / 2 * (right - left) / 1e9;
        }
    }

    private final List<Phase> phases;
    private final long duration;

    WorkloadTimeline(WorkloadProperties config) {
        duration = config.duration().toNanos();
        var values = new ArrayList<Phase>();
        switch (config.pattern()) {
            case STEADY -> values.add(new Phase("steady", 0, duration, config.ratePerSecond(), config.ratePerSecond()));
            case RAMP -> values.add(new Phase("ramp", 0, duration, config.ratePerSecond(), config.peakRatePerSecond()));
            case BURST -> {
                long burstStart = config.burstStart().toNanos();
                long burstEnd = burstStart + config.burstDuration().toNanos();
                long quietEnd = burstEnd + config.effectiveQuietDuration().toNanos();
                add(values, "normal", 0, burstStart, config.ratePerSecond());
                add(values, "burst", burstStart, burstEnd, config.peakRatePerSecond());
                add(values, "quiet", burstEnd, quietEnd, 0);
                add(values, "recovery", quietEnd, duration, config.ratePerSecond());
            }
        }
        phases = List.copyOf(values);
    }

    private static void add(List<Phase> phases, String name, long start, long end, double rate) {
        if (end > start) {
            phases.add(new Phase(name, start, end, rate, rate));
        }
    }

    int phaseIndexAt(long elapsed) {
        for (int i = 0; i < phases.size(); i++) {
            if (elapsed >= phases.get(i).start() && elapsed < phases.get(i).end()) {
                return i;
            }
        }
        return -1;
    }

    double expectedEvents(long start, long end) {
        return phases.stream().mapToDouble(phase -> phase.expectedEvents(start, end)).sum();
    }

    List<Phase> phases() { return phases; }
    long durationNanos() { return duration; }
}
