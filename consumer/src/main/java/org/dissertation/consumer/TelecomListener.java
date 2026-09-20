package org.dissertation.consumer;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TelecomListener {
    private static final Logger LOG = LoggerFactory.getLogger(TelecomListener.class);
    private final EventDecoder decoder;
    private final SimulatedProcessor processor;
    private final ConsumerMetrics metrics;

    public TelecomListener(EventDecoder decoder, SimulatedProcessor processor, ConsumerMetrics metrics) {
        this.decoder = decoder;
        this.processor = processor;
        this.metrics = metrics;
    }

    @KafkaListener(id = "telecom-listener", idIsGroup = false,
            topics = "${consumer.topic:telecom-events}", autoStartup = "${consumer.auto-start:true}")
    public void receive(ConsumerRecord<String, String> record) throws InterruptedException {
        long started = System.nanoTime();
        try {
            var event = decoder.decode(record.value());
            processor.process(event);
            long finished = System.nanoTime();
            int size = record.serializedValueSize() >= 0 ? record.serializedValueSize()
                    : record.value().getBytes(StandardCharsets.UTF_8).length;
            metrics.completed(event.payloadSizeBytes(), size, finished - started,
                    event.createdAt(), System.currentTimeMillis());
        } catch (InterruptedException error) {
            metrics.failed();
            Thread.currentThread().interrupt();
            throw error;
        } catch (RuntimeException error) {
            metrics.failed();
            LOG.error("Processing failed at {}-{} offset={}; consumption will stop",
                    record.topic(), record.partition(), record.offset());
            throw error;
        }
    }
}
