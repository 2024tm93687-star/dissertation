package org.dissertation.producer;

import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

final class SteadyRatePacer {
    private final long intervalNanos;
    private final LongSupplier clock;
    private final LongConsumer pause;
    private long next;
    private long resetCount;
    private long maxLatenessNanos;

    SteadyRatePacer(int ratePerSecond) {
        this(ratePerSecond, System::nanoTime, LockSupport::parkNanos);
    }

    SteadyRatePacer(int ratePerSecond, LongSupplier clock, LongConsumer pause) {
        this.intervalNanos = (long) Math.ceil(1_000_000_000.0 / ratePerSecond);
        this.clock = clock;
        this.pause = pause;
        this.next = clock.getAsLong();
    }

    long awaitSlot() throws InterruptedException {
        long remaining;
        while ((remaining = next - clock.getAsLong()) > 0) {
            checkInterrupted();
            pause.accept(remaining);
        }
        checkInterrupted();
        long now = clock.getAsLong();
        long lateness = now - next;
        maxLatenessNanos = Math.max(maxLatenessNanos, lateness);
        // Keep small wake-up delays from accumulating, but discard missed intervals.
        if (lateness >= intervalNanos) {
            next = now + intervalNanos;
            resetCount++;
        } else {
            next += intervalNanos;
        }
        return now;
    }

    long resetCount() {
        return resetCount;
    }

    long maxLatenessNanos() {
        return maxLatenessNanos;
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Workload interrupted");
        }
    }
}
