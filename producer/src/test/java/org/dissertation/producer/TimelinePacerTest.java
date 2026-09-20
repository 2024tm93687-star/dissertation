package org.dissertation.producer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.dissertation.producer.WorkloadProperties.WorkloadPattern.*;

class TimelinePacerTest {
    static WorkloadProperties burst() {
        return new WorkloadProperties("test", 10, null, 128, 42, 2, "unit", BURST,
                Duration.ofSeconds(4), 40, Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1));
    }

    @Test
    void burstHasExactBoundariesQuietPeriodAndRecovery() throws Exception {
        var timeline = new WorkloadTimeline(burst());
        var clock = new AtomicLong();
        var pacer = new TimelinePacer(timeline, 0, clock::get, clock::addAndGet);
        var metrics = new TimelineMetrics(timeline);
        long elapsed;
        while ((elapsed = pacer.awaitSlot()) >= 0) {
            assertThat(elapsed < 2_000_000_000L || elapsed >= 3_000_000_000L).isTrue();
            metrics.recordSend(elapsed).run();
        }
        assertThat(clock.get()).isEqualTo(4_000_000_000L);
        assertThat(metrics.phases()).extracting(RateObservation::name)
                .containsExactly("normal", "burst", "quiet", "recovery");
        assertThat(metrics.phases()).extracting(RateObservation::sent).containsExactly(10L, 40L, 0L, 10L);
        assertThat(metrics.phases()).extracting(RateObservation::acknowledged).containsExactly(10L, 40L, 0L, 10L);
        assertThat(metrics.windows()).extracting(RateObservation::observedRatePerSecond)
                .containsExactly(10.0, 40.0, 0.0, 10.0);
        assertThat(metrics.phases().get(2).rateErrorPercent()).isNull();
    }

    @Test
    void rampRateIncreasesContinuouslyAndIntegratesCorrectlyPerWindow() throws Exception {
        var config = new WorkloadProperties("test", 10, null, 128, 42, 2, "unit", RAMP,
                Duration.ofSeconds(4), 50, null, null, null);
        var timeline = new WorkloadTimeline(config);
        var clock = new AtomicLong();
        var pacer = new TimelinePacer(timeline, 0, clock::get, clock::addAndGet);
        var slots = new ArrayList<Long>();
        long elapsed;
        while ((elapsed = pacer.awaitSlot()) >= 0) {
            slots.add(elapsed);
        }
        for (int i = 2; i < slots.size(); i++) {
            assertThat(slots.get(i) - slots.get(i - 1)).isLessThanOrEqualTo(slots.get(i - 1) - slots.get(i - 2));
        }
        assertThat(timeline.phases().getFirst().rateAt(2_000_000_000L)).isEqualTo(30);
        assertThat(timeline.expectedEvents(0, 4_000_000_000L)).isEqualTo(120);
        assertThat(new TimelineMetrics(timeline).windows()).extracting(RateObservation::targetAverageRatePerSecond)
                .containsExactly(15.0, 25.0, 35.0, 45.0);
    }

    @Test
    void delaySkipsExpiredPhasesRatherThanReplayingThem() throws Exception {
        var timeline = new WorkloadTimeline(burst());
        var clock = new AtomicLong();
        var pacer = new TimelinePacer(timeline, 0, clock::get, clock::addAndGet);
        assertThat(pacer.awaitSlot()).isZero();
        clock.set(2_500_000_000L);
        assertThat(pacer.awaitSlot()).isEqualTo(3_000_000_000L);
        assertThat(pacer.awaitSlot()).isEqualTo(3_100_000_000L);
        clock.set(5_000_000_000L);
        assertThat(pacer.awaitSlot()).isEqualTo(-1);
    }

    @Test
    void delayWhileSleepingCannotLeakAnEventIntoQuietTimeOrPastTheDeadline() throws Exception {
        var timeline = new WorkloadTimeline(burst());
        var clock = new AtomicLong(1_990_000_000L);
        var pacer = new TimelinePacer(timeline, 0, clock::get, n -> clock.addAndGet(n + 10_000_000));
        assertThat(pacer.awaitSlot()).isEqualTo(1_990_000_000L);
        assertThat(pacer.awaitSlot()).isEqualTo(3_010_000_000L);
        clock.set(3_990_000_000L);
        pacer.awaitSlot();
        assertThat(pacer.awaitSlot()).isEqualTo(-1);
    }

    @Test
    void deadlineShorterThanAnIntervalStillEndsOnTime() throws Exception {
        var config = new WorkloadProperties("test", 1, null, 128, 42, 2, "unit", STEADY,
                Duration.ofMillis(250), null, null, null, null);
        var timeline = new WorkloadTimeline(config);
        var clock = new AtomicLong();
        var pacer = new TimelinePacer(timeline, 0, clock::get, clock::addAndGet);
        var metrics = new TimelineMetrics(timeline);
        metrics.recordSend(pacer.awaitSlot()).run();
        assertThat(pacer.awaitSlot()).isEqualTo(-1);
        assertThat(clock.get()).isEqualTo(250_000_000L);
        assertThat(metrics.windows().getFirst().observedRatePerSecond()).isEqualTo(4);
    }

    @Test
    void smallOversleepsDoNotAccumulateAndLargeDelaysReset() throws Exception {
        var config = new WorkloadProperties("test", 20, null, 128, 42, 2, "unit", STEADY,
                Duration.ofSeconds(20), null, null, null, null);
        var clock = new AtomicLong();
        var pacer = new TimelinePacer(new WorkloadTimeline(config), 0, clock::get,
                n -> clock.addAndGet(n + 10_000_000));
        for (int i = 0; i < 201; i++) {
            pacer.awaitSlot();
        }
        assertThat(clock.get()).isEqualTo(10_010_000_000L);
        assertThat(pacer.resetCount()).isZero();
        clock.addAndGet(1_000_000_000L);
        pacer.awaitSlot();
        assertThat(pacer.resetCount()).isEqualTo(1);
        assertThat(pacer.maxLatenessNanos()).isGreaterThan(900_000_000);
    }

    @Test
    void windowTargetsIntegrateAcrossFractionalPhaseBoundaries() {
        var config = new WorkloadProperties("test", 10, null, 128, 42, 2, "unit", BURST,
                Duration.ofMillis(2500), 30, Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofMillis(500));
        var metrics = new TimelineMetrics(new WorkloadTimeline(config));
        assertThat(metrics.windows()).extracting(RateObservation::targetAverageRatePerSecond)
                .containsExactly(20.0, 15.0, 10.0);
        assertThat(metrics.windows()).extracting(RateObservation::rateErrorPercent)
                .containsExactly(-100.0, -100.0, -100.0);
    }

    @Test
    void interruptionDuringQuietWaitIsHonored() throws Exception {
        var clock = new AtomicLong(2_000_000_000L);
        var pacer = new TimelinePacer(new WorkloadTimeline(burst()), 0, clock::get,
                n -> Thread.currentThread().interrupt());
        try {
            assertThatThrownBy(pacer::awaitSlot).isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }
}
