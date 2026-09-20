package org.dissertation.producer;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SteadyRatePacerTest {
    @Test
    void repeatedSmallOversleepsDoNotAccumulateIntoRateDrift() throws Exception {
        var clock = new AtomicLong();
        var pacer = new SteadyRatePacer(20, clock::get,
                remaining -> clock.addAndGet(remaining + 10_000_000));
        for (int i = 0; i < 201; i++) {
            pacer.awaitSlot();
        }
        assertThat(clock.get()).isEqualTo(10_010_000_000L);
        assertThat(pacer.resetCount()).isZero();
        assertThat(pacer.maxLatenessNanos()).isEqualTo(10_000_000);
    }

    @Test
    void spacesEventsAndDoesNotCatchUpAfterAStall() throws Exception {
        var clock = new AtomicLong();
        var pacer = new SteadyRatePacer(100, clock::get, clock::addAndGet);
        pacer.awaitSlot();
        assertThat(clock.get()).isZero();
        pacer.awaitSlot();
        assertThat(clock.get()).isEqualTo(10_000_000);
        clock.set(1_000_000_000);
        pacer.awaitSlot();
        pacer.awaitSlot();
        assertThat(clock.get()).isEqualTo(1_010_000_000);
        assertThat(pacer.resetCount()).isEqualTo(1);
        assertThat(pacer.maxLatenessNanos()).isEqualTo(980_000_000);
    }

    @Test
    void latenessOfExactlyOneIntervalResetsInsteadOfReleasingTwoEventsTogether() throws Exception {
        var clock = new AtomicLong();
        var pacer = new SteadyRatePacer(20, clock::get, clock::addAndGet);
        pacer.awaitSlot();
        clock.set(100_000_000);
        assertThat(pacer.awaitSlot()).isEqualTo(100_000_000);
        assertThat(pacer.awaitSlot()).isEqualTo(150_000_000);
        assertThat(pacer.resetCount()).isEqualTo(1);
    }

    @Test
    void smallAmountsOfEventProcessingDoNotShiftTheSchedule() throws Exception {
        var clock = new AtomicLong();
        var pacer = new SteadyRatePacer(20, clock::get, clock::addAndGet);
        for (int i = 0; i < 201; i++) {
            assertThat(pacer.awaitSlot()).isEqualTo(i * 50_000_000L);
            clock.addAndGet(5_000_000);
        }
        assertThat(pacer.resetCount()).isZero();
    }

    @Test
    void toleratesMonotonicClockWraparound() throws Exception {
        long start = Long.MAX_VALUE - 5_000_000;
        var clock = new AtomicLong(start);
        var pacer = new SteadyRatePacer(100, clock::get, clock::addAndGet);
        assertThat(pacer.awaitSlot()).isEqualTo(start);
        assertThat(pacer.awaitSlot() - start).isEqualTo(10_000_000);
        assertThat(pacer.resetCount()).isZero();
    }

    @Test
    void interruptionDuringWaitPreservesTheInterruptFlag() {
        var clock = new AtomicLong();
        var pacer = new SteadyRatePacer(100, clock::get,
                remaining -> Thread.currentThread().interrupt());
        try {
            pacer.awaitSlot();
            assertThatThrownBy(pacer::awaitSlot).isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } catch (InterruptedException error) {
            throw new AssertionError(error);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void toleratesEarlyWakeups() throws Exception {
        var clock = new AtomicLong();
        var pacer = new SteadyRatePacer(100, clock::get,
                remaining -> clock.addAndGet(Math.max(1, remaining / 2)));
        pacer.awaitSlot();
        pacer.awaitSlot();
        assertThat(clock.get()).isEqualTo(10_000_000);
    }

    @Test
    void honorsInterruption() {
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> new SteadyRatePacer(100).awaitSlot())
                    .isInstanceOf(InterruptedException.class);
        } finally {
            Thread.interrupted();
        }
    }
}
