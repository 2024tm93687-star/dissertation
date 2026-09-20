package org.dissertation.consumer;

import java.nio.charset.StandardCharsets;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class EventDecoder {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final Validator validator;

    public EventDecoder(Validator validator) {
        this.validator = validator;
    }

    TelecomEvent decode(String json) {
        if (json == null) {
            throw new IllegalArgumentException("Tombstones are not telecom events");
        }
        var event = mapper.readValue(json, TelecomEvent.class);
        if (event == null || !validator.validate(event).isEmpty()) {
            throw new IllegalArgumentException("Missing or invalid telecom event fields");
        }
        if (event.payload().getBytes(StandardCharsets.UTF_8).length != event.payloadSizeBytes()) {
            throw new IllegalArgumentException("Payload byte length does not match payloadSizeBytes");
        }
        return event;
    }
}
