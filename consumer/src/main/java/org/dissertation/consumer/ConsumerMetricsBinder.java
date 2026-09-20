package org.dissertation.consumer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class ConsumerMetricsBinder {
    public ConsumerMetricsBinder(MeterRegistry registry, ConsumerMetrics metrics,
            ConsumerSettings settings, Environment environment) {
        var tags = Tags.of("topic", settings.topic(),
                "group", environment.getRequiredProperty("spring.kafka.consumer.group-id"));
        gauge(registry, "dissertation.consumer.processed", "Completed processing attempts", tags,
                metrics, snapshot -> snapshot.processed());
        gauge(registry, "dissertation.consumer.failed", "Failed processing attempts", tags,
                metrics, snapshot -> snapshot.failed());
        gauge(registry, "dissertation.consumer.payload.bytes", "Processed payload bytes", tags,
                metrics, snapshot -> snapshot.processedPayloadBytes());
        gauge(registry, "dissertation.consumer.value.bytes", "Processed Kafka value bytes", tags,
                metrics, snapshot -> snapshot.processedValueBytes());
        gauge(registry, "dissertation.consumer.assigned.partitions", "Currently assigned Kafka partitions", tags,
                metrics, snapshot -> snapshot.assignedPartitions().size());
        gauge(registry, "dissertation.consumer.future.timestamps", "Processed records excluded from latency due to future timestamps",
                tags, metrics, snapshot -> snapshot.futureTimestampEvents());
        gauge(registry, "dissertation.consumer.processing.p95.millis", "Processing time p95 in milliseconds", tags,
                metrics, snapshot -> valueOrNan(snapshot.processingTime().p95Millis()));
        gauge(registry, "dissertation.consumer.processing.p99.millis", "Processing time p99 in milliseconds", tags,
                metrics, snapshot -> valueOrNan(snapshot.processingTime().p99Millis()));
        gauge(registry, "dissertation.consumer.latency.p95.millis", "End-to-end latency p95 in milliseconds", tags,
                metrics, snapshot -> valueOrNan(snapshot.endToEndLatency().p95Millis()));
        gauge(registry, "dissertation.consumer.latency.p99.millis", "End-to-end latency p99 in milliseconds", tags,
                metrics, snapshot -> valueOrNan(snapshot.endToEndLatency().p99Millis()));
    }

    private static void gauge(MeterRegistry registry, String name, String description, Tags tags,
            ConsumerMetrics metrics, SnapshotValue value) {
        Gauge.builder(name, metrics, item -> value.apply(item.current()))
                .description(description)
                .tags(tags)
                .register(registry);
    }

    private static double valueOrNan(Double value) {
        return value == null ? Double.NaN : value;
    }

    @FunctionalInterface
    private interface SnapshotValue {
        double apply(ConsumerMetrics.Snapshot snapshot);
    }
}
