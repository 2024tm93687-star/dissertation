package org.dissertation.producer;

public record TelecomEvent(
        String eventId,
        String runId,
        long sequence,
        String eventType,
        long createdAt,
        String subscriberId,
        String customerRegion,
        int payloadSizeBytes,
        String payload,
        String workloadPhase,
        long workloadElapsedNanos) {
}
