package org.dissertation.consumer;

import java.time.Duration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "consumer", ignoreUnknownFields = false)
public record ConsumerSettings(
        @DefaultValue("telecom-events") @NotBlank
        @Pattern(regexp = "[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}") String topic,
        @DefaultValue("1") @Min(1) @Max(32) int concurrency,
        @DefaultValue("1000") @Min(0) @Max(1000000) int cpuIterations,
        @DefaultValue("0") @Min(0) @Max(10000) int processingDelayMs,
        @DefaultValue("5000") @Min(100) @Max(60000) int reportIntervalMs,
        @DefaultValue("true") boolean autoStart,
        Duration runDuration) {

    @AssertTrue(message = "run-duration must be positive and no longer than one hour")
    public boolean isRunDurationValid() {
        return runDuration == null || (!runDuration.isNegative() && !runDuration.isZero()
                && runDuration.compareTo(Duration.ofHours(1)) <= 0);
    }
}
