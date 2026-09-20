package org.dissertation.producer;

import java.util.Random;

final class EventFactory {
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final String[] REGIONS = {"north", "south", "east", "west"};
    private final String runId;
    private final int payloadSize;
    private final Random random;

    EventFactory(String runId, int payloadSize, long seed) {
        this.runId = runId;
        this.payloadSize = payloadSize;
        this.random = new Random(seed);
    }

    TelecomEvent create(long sequence, long createdAt) {
        return create(sequence, createdAt, "steady", 0);
    }

    TelecomEvent create(long sequence, long createdAt, String phase, long elapsedNanos) {
        char[] payload = new char[payloadSize];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = ALPHABET.charAt(random.nextInt(ALPHABET.length()));
        }
        return new TelecomEvent(runId + ":" + sequence, runId, sequence, "CALL_RECORD",
                createdAt, "subscriber-" + sequence % 1000,
                REGIONS[(int) (sequence % REGIONS.length)], payloadSize, new String(payload), phase, elapsedNanos);
    }
}
