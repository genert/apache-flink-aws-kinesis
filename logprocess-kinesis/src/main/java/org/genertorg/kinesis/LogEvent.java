package org.genertorg.kinesis;

import java.util.Objects;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@DefaultCoder(SerializableCoder.class)
public class LogEvent extends Event {
    public final int userId;
    public final String status;

    public LogEvent() {
        userId = 0;
        status = "";
    }

    public int getUserId() {
        return userId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEvent tripEvent = (LogEvent) o;
        return userId == tripEvent.userId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId);
    }
}