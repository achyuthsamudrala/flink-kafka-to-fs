package org.ing.wbaa.functions;

import org.ing.wbaa.serde.Event;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class EventBucketAssigner implements BucketAssigner<Event, String> {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("'yyyy_mm_dd='yyyy-MM-dd");

    @Override
    public String getBucketId(Event event, Context context) {
        return Instant
                .ofEpochMilli(event.getEventTime())
                .atZone(ZoneOffset.UTC)
                .format(DATE_TIME_FORMATTER);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
