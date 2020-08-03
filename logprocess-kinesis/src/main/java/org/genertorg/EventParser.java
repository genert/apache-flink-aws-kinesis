package org.genertorg;

import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.genertorg.kinesis.Event;
import org.genertorg.kinesis.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventParser {
    private static final Logger LOG = LoggerFactory.getLogger(EventParser.class);

    public static class KinesisParser extends DoFn<KinesisRecord, LogEvent> {
        @ProcessElement
        public void processElement(@Element KinesisRecord record, OutputReceiver<LogEvent> out) {
            try {
                Event event = Event.parseEvent(record.getDataAsBytes());

                if (LogEvent.class.isAssignableFrom(event.getClass())) {
                    LogEvent trip = (LogEvent) event;

                    out.output(trip);
                }
            } catch (Exception e) {
                //just ignore the event
                LOG.warn("failed to parse event: {}", e.getLocalizedMessage());
            }
        }
    }
}