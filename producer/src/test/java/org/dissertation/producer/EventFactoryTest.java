package org.dissertation.producer;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.assertThat;

class EventFactoryTest {
    @Test
    void payloadIsRepeatableAndExactlyTheRequestedUtf8Size() {
        var first = new EventFactory("test", 1024, 42);
        var second = new EventFactory("test", 1024, 42);
        var mapper = JsonMapper.builder().build();
        for (int i = 0; i < 3; i++) {
            var event = first.create(i, 1234);
            assertThat(event).isEqualTo(second.create(i, 1234));
            assertThat(event.payload().getBytes(StandardCharsets.UTF_8)).hasSize(1024);
            assertThat(event.eventId()).isEqualTo("test:" + i);
            String json = mapper.writeValueAsString(event);
            assertThat(mapper.readValue(json, TelecomEvent.class)).isEqualTo(event);
            assertThat(json.getBytes(StandardCharsets.UTF_8).length).isGreaterThan(1024);
        }
    }

    @Test
    void supportsEmptyPayloadAndDifferentSeeds() {
        assertThat(new EventFactory("test", 0, 42).create(0, 0).payload()).isEmpty();
        assertThat(new EventFactory("test", 256, 42).create(0, 0).payload())
                .isNotEqualTo(new EventFactory("test", 256, 43).create(0, 0).payload());
    }
}
