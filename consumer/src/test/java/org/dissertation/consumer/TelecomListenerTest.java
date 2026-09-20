package org.dissertation.consumer;

import jakarta.validation.Validation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class TelecomListenerTest {
    @Test
    void onlySuccessfulRecordsContributeToThroughput() throws Exception {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var metrics = new ConsumerMetrics();
            var listener = new TelecomListener(new EventDecoder(factory.getValidator()),
                    new SimulatedProcessor(SimulatedProcessorTest.settings(0, 0)), metrics);
            listener.receive(new ConsumerRecord<>("test", 0, 0, "key", """
                    {"eventId":"e","runId":"r","createdAt":1,"payloadSizeBytes":1,"payload":"x"}
                    """));
            assertThatThrownBy(() -> listener.receive(new ConsumerRecord<>("test", 0, 1, "key", "bad")))
                    .isInstanceOf(RuntimeException.class);
            assertThat(metrics.snapshot().processed()).isEqualTo(1);
            assertThat(metrics.snapshot().failed()).isEqualTo(1);
        }
    }
}
