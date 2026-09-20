package org.dissertation.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TelecomEvent(@NotBlank String eventId, @NotBlank String runId,
        @Positive long createdAt, @NotNull @Min(0) Integer payloadSizeBytes,
        @NotNull String payload) {
}
