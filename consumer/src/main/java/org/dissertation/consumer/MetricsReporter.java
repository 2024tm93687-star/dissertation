package org.dissertation.consumer;

import java.util.UUID;
import jakarta.annotation.PreDestroy;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class MetricsReporter {
    private final ConsumerMetrics metrics;
    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerSettings settings;
    private final String groupId;
    private final String instanceId = UUID.randomUUID().toString();
    private final JsonMapper mapper = JsonMapper.builder().build();

    public MetricsReporter(ConsumerMetrics metrics, KafkaListenerEndpointRegistry registry,
            ConsumerSettings settings, Environment environment) {
        this.metrics = metrics;
        this.registry = registry;
        this.settings = settings;
        this.groupId = environment.getRequiredProperty("spring.kafka.consumer.group-id");
    }

    @Scheduled(initialDelayString = "${consumer.report-interval-ms:5000}",
            fixedDelayString = "${consumer.report-interval-ms:5000}")
    public synchronized void report() { write(false); }

    @PreDestroy
    public synchronized void finalReport() { write(true); }

    private void write(boolean finalSnapshot) {
        var listener = registry.getListenerContainer("telecom-listener");
        var report = new Report(instanceId, settings.topic(), groupId, System.currentTimeMillis(),
                finalSnapshot, listener != null && listener.isRunning(), metrics.snapshot());
        System.out.println("CONSUMER_METRICS " + mapper.writeValueAsString(report));
    }

    public record Report(String instanceId, String topic, String groupId, long timestamp,
            boolean finalSnapshot, boolean listenerRunning, ConsumerMetrics.Snapshot metrics) { }
}
