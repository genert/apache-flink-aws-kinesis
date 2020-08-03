package logprocessing;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.Properties;

public class LogProcessingJob {
    private static final ObjectMapper jsonParser = new ObjectMapper();
    private static final String region = "us-east-1";
    private static final String inputStreamName = "genert-testib-apache-flinki";
    private static final String s3SinkPath = "s3a://genert-testib-apache-flinki/data";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    private static StreamingFileSink<Tuple2> createS3SinkFromStaticConfig() {
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix") // HOW TO GET user_id here?
                .withPartSuffix(".json")
                .build();

        return StreamingFileSink
                .forRowFormat(new Path(s3SinkPath), new SimpleStringEncoder<Tuple2>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(config)
                .build();
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* if you would like to use runtime configuration properties, uncomment the lines below
         * DataStream<String> input = createSourceFromApplicationProperties(env);
         */
        DataStream<String> input = createSourceFromStaticConfig(env);

        input.map(value -> { // Parse the JSON
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            return new Tuple2(jsonNode.get("user_id").asInt(),
                    jsonNode.get("status").asText());
        }).returns(Types.TUPLE(Types.INT, Types.STRING))
                .keyBy(event -> event.f0) // partition by user_id
                .addSink(createS3SinkFromStaticConfig());

        env.execute("Process log files");
    }
}
