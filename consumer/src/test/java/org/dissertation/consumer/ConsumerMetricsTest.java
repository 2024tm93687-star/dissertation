package org.dissertation.consumer;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ConsumerMetricsTest {
    @Test
    void measuresIntervalThroughputAndCumulativePercentiles() {
        var clock = new AtomicLong();
        var metrics = new ConsumerMetrics(clock::get);
        assertThat(metrics.snapshot().intervalThroughputPerSecond()).isNull();
        for (int i = 1; i <= 100; i++) {
            metrics.completed(10, 100, i * 1_000_000L, 1000, 1000 + i);
        }
        clock.set(2_000_000_000L);
        var snapshot = metrics.snapshot();
        assertThat(snapshot.processed()).isEqualTo(100);
        assertThat(snapshot.processedPayloadBytes()).isEqualTo(1000);
        assertThat(snapshot.processedValueBytes()).isEqualTo(10000);
        assertThat(snapshot.intervalThroughputPerSecond()).isEqualTo(50);
        assertThat(snapshot.endToEndLatency().samples()).isEqualTo(100);
        assertThat(snapshot.endToEndLatency().meanMillis()).isCloseTo(50.5, within(0.1));
        assertThat(snapshot.endToEndLatency().p95Millis()).isCloseTo(95, within(0.1));
        assertThat(snapshot.endToEndLatency().p99Millis()).isCloseTo(99, within(0.1));
        clock.set(3_000_000_000L);
        assertThat(metrics.snapshot().intervalThroughputPerSecond()).isZero();
    }

    @Test
    void excludesClockSkewAndOutOfRangeSamplesWithoutFakingZeroLatency() {
        var metrics = new ConsumerMetrics();
        assertThat(metrics.snapshot().endToEndLatency().p95Millis()).isNull();
        metrics.completed(0, 10, 1000, 2000, 1000);
        metrics.completed(0, 10, (ConsumerMetrics.MAX_MICROS + 1) * 1000,
                1, ConsumerMetrics.MAX_MICROS / 1000 + 2);
        metrics.failed();
        var snapshot = metrics.snapshot();
        assertThat(snapshot.processed()).isEqualTo(2);
        assertThat(snapshot.failed()).isEqualTo(1);
        assertThat(snapshot.futureTimestampEvents()).isEqualTo(1);
        assertThat(snapshot.endToEndLatency().samples()).isZero();
        assertThat(snapshot.endToEndLatency().excludedOutOfRange()).isEqualTo(1);
        assertThat(snapshot.processingTime().excludedOutOfRange()).isEqualTo(1);
    }

    @Test
    void tracksAssignmentChangesWithoutCallingThemRebalanceDurations() {
        var metrics = new ConsumerMetrics();
        var first = new TopicPartition("test", 0);
        var second = new TopicPartition("test", 1);
        metrics.onPartitionsAssigned(List.of(first, second));
        metrics.onPartitionsRevoked(List.of(first));
        assertThat(metrics.snapshot().assignedPartitions()).containsExactly("test-1");
        metrics.onPartitionsLost(List.of(second));
        var snapshot = metrics.snapshot();
        assertThat(snapshot.assignedPartitions()).isEmpty();
        assertThat(snapshot.assignmentEvents()).isEqualTo(1);
        assertThat(snapshot.revocationEvents()).isEqualTo(1);
        assertThat(snapshot.lostEvents()).isEqualTo(1);
    }
}
