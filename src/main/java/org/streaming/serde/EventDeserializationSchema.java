package org.streaming.serde;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;
import java.time.Instant;


public class EventDeserializationSchema implements KeyedDeserializationSchema<Event> {

    private final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Event deserialize(byte[] key, byte[] message, String topic, int partition, long offset) throws IOException {
        Long currentTime = Instant.now().toEpochMilli();

        if (message.length == 0)
            return new Event(currentTime, "NULL EVENT");

        try {
            return MAPPER.readValue(message, Event.class);
        } catch (Exception e) {
            return new Event(currentTime, "NON SERIALIZABLE EVENT");
        }

    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }


    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }

}
