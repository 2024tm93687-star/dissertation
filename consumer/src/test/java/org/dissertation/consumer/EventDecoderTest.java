package org.dissertation.consumer;

import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class EventDecoderTest {
    @Test
    void acceptsLegacyAndExtendedEventsWithExactUtf8PayloadLength() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var decoder = new EventDecoder(factory.getValidator());
            var event = decoder.decode("""
                    {"eventId":"test:0","runId":"test","createdAt":1000,
                     "payloadSizeBytes":2,"payload":"\u00e9","workloadPhase":"burst","sequence":0}
                    """);
            assertThat(event.payloadSizeBytes()).isEqualTo(2);
            assertThat(decoder.decode("""
                    {"eventId":"test:1","runId":"test","createdAt":1000,"payloadSizeBytes":0,"payload":""}
                    """).payload()).isEmpty();
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"null", "broken json", "{}",
            "{\"eventId\":\"e\",\"runId\":\"r\",\"createdAt\":0,\"payloadSizeBytes\":0,\"payload\":\"\"}",
            "{\"eventId\":\"e\",\"runId\":\"r\",\"createdAt\":1,\"payloadSizeBytes\":3,\"payload\":\"x\"}",
            "{\"eventId\":\"e\",\"runId\":\"r\",\"createdAt\":1,\"payload\":\"\"}"})
    void rejectsInvalidRecords(String json) {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            assertThatThrownBy(() -> new EventDecoder(factory.getValidator()).decode(json))
                    .isInstanceOf(RuntimeException.class);
        }
    }
}
