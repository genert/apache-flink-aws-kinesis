package org.genertorg.kinesis;

import com.google.gson.*;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.joda.time.Instant;

@DefaultCoder(SerializableCoder.class)
public abstract class Event implements Serializable {
    private static final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .registerTypeAdapter(Instant.class, (JsonDeserializer<Instant>) (json, typeOfT, context) -> Instant.parse(json.getAsString()))
            .create();

    public static Event parseEvent(String event) {
        return parseEvent(event.getBytes(StandardCharsets.UTF_8));
    }

    public static Event parseEvent(byte[] event) {
        JsonReader jsonReader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(event)));
        JsonElement jsonElement = Streams.parse(jsonReader);

        return gson.fromJson(jsonElement, LogEvent.class);
    }
}