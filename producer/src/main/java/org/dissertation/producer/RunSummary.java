package org.dissertation.producer;

import java.util.List;

public record RunSummary(
        String runId,
        String topic,
        String pattern,
        long seed,
        int targetRatePerSecond,
        int payloadSizeBytes,
        Long requested,
        long acknowledged,
        long serializedValueBytes,
        long startedAt,
        long finishedAt,
        double elapsedSeconds,
        double acknowledgedRatePerSecond,
        double eventStartSpanSeconds,
        Double eventStartRatePerSecond,
        long pacingResetCount,
        double maxPacingLatenessMillis,
        Double requestedDurationSeconds,
        long sent,
        List<RateObservation> phases,
        List<RateObservation> rateWindows) {
}
