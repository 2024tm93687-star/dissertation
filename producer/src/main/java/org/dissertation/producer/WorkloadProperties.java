package org.dissertation.producer;

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
@ConfigurationProperties(prefix = "workload", ignoreUnknownFields = false)
public record WorkloadProperties(
        @DefaultValue("telecom-events") @NotBlank
        @Pattern(regexp = "[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}") String topic,
        @DefaultValue("100") @Min(1) @Max(100000) int ratePerSecond,
        @Min(1) @Max(100000000) Long eventCount,
        @DefaultValue("1024") @Min(0) @Max(262144) int payloadSizeBytes,
        @DefaultValue("42") long seed,
        @DefaultValue("256") @Min(1) @Max(1024) int maxPendingSends,
        @DefaultValue("") @Pattern(regexp = "[a-zA-Z0-9_-]{0,64}") String runId,
        @DefaultValue("STEADY") WorkloadPattern pattern,
        Duration duration,
        @Min(1) @Max(100000) Integer peakRatePerSecond,
        Duration burstStart,
        Duration burstDuration,
        Duration quietDuration) {

    public enum WorkloadPattern { STEADY, RAMP, BURST }

    long effectiveEventCount() {
        return eventCount == null ? 1000 : eventCount;
    }

    Duration effectiveQuietDuration() {
        return quietDuration == null ? Duration.ZERO : quietDuration;
    }

    @AssertTrue(message = "Use event-count OR duration (0 < duration <= 1h). Ramp requires duration and peak-rate-per-second >= rate-per-second. Burst also requires nonnegative burst-start, positive burst-duration, and optional nonnegative quiet-duration fitting inside duration. Phase options are only valid for their pattern.")
    public boolean isTimelineValid() {
        if (pattern == null) {
            return false;
        }
        if (duration != null && (duration.isNegative() || duration.isZero()
                || duration.compareTo(Duration.ofHours(1)) > 0 || eventCount != null)) {
            return false;
        }
        if (pattern == WorkloadPattern.STEADY) {
            return peakRatePerSecond == null && burstStart == null && burstDuration == null && quietDuration == null;
        }
        if (duration == null || peakRatePerSecond == null || peakRatePerSecond < ratePerSecond) {
            return false;
        }
        if (pattern == WorkloadPattern.RAMP) {
            return burstStart == null && burstDuration == null && quietDuration == null;
        }
        if (burstStart == null || burstDuration == null || burstStart.isNegative()
                || burstDuration.isNegative() || burstDuration.isZero()
                || effectiveQuietDuration().isNegative()
                || burstStart.compareTo(duration) > 0 || burstDuration.compareTo(duration) > 0
                || effectiveQuietDuration().compareTo(duration) > 0) {
            return false;
        }
        return burstStart.plus(burstDuration).plus(effectiveQuietDuration()).compareTo(duration) <= 0;
    }
}
