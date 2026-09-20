package org.dissertation.consumer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import static org.assertj.core.api.Assertions.*;

class ConsumerMetricsBinderTest {
    @Test
    void exposesDissertationConsumerMetrics() {
        var registry = new SimpleMeterRegistry();
        var metrics = new ConsumerMetrics();
        metrics.completed(256, 512, 5_000_000, 1000, 1010);
        var settings = new ConsumerSettings("telecom-events", 1, 1000, 0, 5000, true, null);
        var environment = new MockEnvironment()
                .withProperty("spring.kafka.consumer.group-id", "test-group");

        new ConsumerMetricsBinder(registry, metrics, settings, environment);

        assertThat(registry.find("dissertation.consumer.processed")
                .tag("topic", "telecom-events")
                .tag("group", "test-group")
                .gauge()
                .value()).isEqualTo(1);
        assertThat(registry.find("dissertation.consumer.payload.bytes").gauge().value()).isEqualTo(256);
        assertThat(registry.find("dissertation.consumer.latency.p95.millis").gauge().value()).isGreaterThan(0);
    }
}
