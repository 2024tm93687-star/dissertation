package org.dissertation.consumer;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongSupplier;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

@Component
public class ConsumerMetrics implements ConsumerRebalanceListener {
    static final long MAX_MICROS = 7L * 24 * 60 * 60 * 1_000_000;
    private final LongSupplier clock;
    private final long startNanos;
    private long lastSnapshotNanos;
    private long lastProcessed;
    private long processed;
    private long failed;
    private long payloadBytes;
    private long valueBytes;
    private long futureTimestamps;
    private long assignmentEvents;
    private long revocationEvents;
    private long lostEvents;
    private final Set<TopicPartition> assigned = new HashSet<>();
    private final Distribution processing = new Distribution();
    private final Distribution latency = new Distribution();

    public ConsumerMetrics() { this(System::nanoTime); }

    ConsumerMetrics(LongSupplier clock) {
        this.clock = clock;
        startNanos = clock.getAsLong();
        lastSnapshotNanos = startNanos;
    }

    synchronized void completed(int payloadSize, int valueSize, long processingNanos, long createdAt, long finishedAt) {
        processed++;
        payloadBytes += payloadSize;
        valueBytes += valueSize;
        processing.record(processingNanos / 1000);
        if (finishedAt < createdAt) {
            futureTimestamps++;
        } else {
            long millis = finishedAt - createdAt;
            latency.record(millis > MAX_MICROS / 1000 ? MAX_MICROS + 1 : millis * 1000);
        }
    }

    synchronized void failed() { failed++; }

    @Override
    public synchronized void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assignmentEvents++;
        assigned.addAll(partitions);
    }

    @Override
    public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        revocationEvents++;
        assigned.removeAll(partitions);
    }

    @Override
    public synchronized void onPartitionsLost(Collection<TopicPartition> partitions) {
        lostEvents++;
        assigned.removeAll(partitions);
    }

    public synchronized Snapshot snapshot() {
        return snapshot(true);
    }

    public synchronized Snapshot current() {
        return snapshot(false);
    }

    private Snapshot snapshot(boolean advanceInterval) {
        long now = clock.getAsLong();
        double seconds = (now - startNanos) / 1e9;
        double interval = (now - lastSnapshotNanos) / 1e9;
        Double rate = interval > 0 ? (processed - lastProcessed) / interval : null;
        if (advanceInterval) {
            lastProcessed = processed;
            lastSnapshotNanos = now;
        }
        return new Snapshot(processed, failed, payloadBytes, valueBytes, seconds,
                seconds > 0 ? processed / seconds : null, interval, rate,
                processing.summary(), latency.summary(), futureTimestamps,
                assignmentEvents, revocationEvents, lostEvents,
                assigned.stream().map(TopicPartition::toString).sorted().toList());
    }

    public record Snapshot(long processed, long failed, long processedPayloadBytes, long processedValueBytes,
            double elapsedSeconds, Double averageThroughputPerSecond, double intervalSeconds,
            Double intervalThroughputPerSecond, DistributionSummary processingTime,
            DistributionSummary endToEndLatency, long futureTimestampEvents,
            long assignmentEvents, long revocationEvents, long lostEvents, List<String> assignedPartitions) { }

    public record DistributionSummary(long samples, long excludedOutOfRange, Double meanMillis,
            Double p95Millis, Double p99Millis, Double maxMillis) { }

    private static final class Distribution {
        private final Histogram histogram = new Histogram(1, MAX_MICROS, 3);
        private long excluded;

        private Distribution() { histogram.setAutoResize(false); }

        private void record(long micros) {
            if (micros < 0 || micros > MAX_MICROS) {
                excluded++;
            } else {
                histogram.recordValue(micros);
            }
        }

        private DistributionSummary summary() {
            long count = histogram.getTotalCount();
            return new DistributionSummary(count, excluded,
                    count == 0 ? null : histogram.getMean() / 1000,
                    count == 0 ? null : histogram.getValueAtPercentile(95) / 1000.0,
                    count == 0 ? null : histogram.getValueAtPercentile(99) / 1000.0,
                    count == 0 ? null : histogram.getMaxValue() / 1000.0);
        }
    }
}
