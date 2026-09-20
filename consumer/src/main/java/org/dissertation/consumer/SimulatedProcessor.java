package org.dissertation.consumer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.springframework.stereotype.Component;

@Component
public class SimulatedProcessor {
    private final ConsumerSettings settings;
    private volatile int lastChecksum;

    public SimulatedProcessor(ConsumerSettings settings) {
        this.settings = settings;
    }

    void process(TelecomEvent event) throws InterruptedException {
        if (settings.cpuIterations() > 0) {
            try {
                var digest = MessageDigest.getInstance("SHA-256");
                byte[] value = event.payload().getBytes(StandardCharsets.UTF_8);
                for (int i = 0; i < settings.cpuIterations(); i++) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Processing interrupted");
                    }
                    value = digest.digest(value);
                }
                lastChecksum = value[0];
            } catch (NoSuchAlgorithmException error) {
                throw new IllegalStateException("SHA-256 is unavailable", error);
            }
        }
        if (settings.processingDelayMs() > 0) {
            Thread.sleep(settings.processingDelayMs());
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Processing interrupted");
        }
    }

    int lastChecksum() { return lastChecksum; }
}
