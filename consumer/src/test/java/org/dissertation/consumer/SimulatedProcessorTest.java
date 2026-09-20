package org.dissertation.consumer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SimulatedProcessorTest {
    @Test
    void performsConfiguredHashIterations() throws Exception {
        var processor = new SimulatedProcessor(settings(2, 0));
        var event = new TelecomEvent("e", "r", 1, 3, "abc");
        processor.process(event);
        var digest = MessageDigest.getInstance("SHA-256");
        byte[] expected = digest.digest(digest.digest("abc".getBytes(StandardCharsets.UTF_8)));
        assertThat(processor.lastChecksum()).isEqualTo(expected[0]);
    }

    @Test
    void delayIsIncludedInProcessingAndCanBeInterrupted() throws Exception {
        var event = new TelecomEvent("e", "r", 1, 0, "");
        long start = System.nanoTime();
        new SimulatedProcessor(settings(0, 10)).process(event);
        assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(10_000_000);
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> new SimulatedProcessor(settings(1, 0)).process(event))
                    .isInstanceOf(InterruptedException.class);
        } finally {
            Thread.interrupted();
        }
    }

    static ConsumerSettings settings(int iterations, int delay) {
        return new ConsumerSettings("test", 1, iterations, delay, 5000, true, null);
    }
}
