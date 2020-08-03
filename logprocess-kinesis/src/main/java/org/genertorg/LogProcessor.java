package org.genertorg;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.commons.lang3.ArrayUtils;
import com.amazonaws.regions.Regions;
import org.genertorg.kinesis.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.io.kinesis.KinesisIO;

public class LogProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LogProcessor.class);

    public static void main(String[] args) {
        String[] kinesisArgs = LogProcessorOptions.argsFromKinesisApplicationProperties(args, "BeamApplicationProperties");

        LogProcessorOptions options = PipelineOptionsFactory.fromArgs(ArrayUtils.addAll(args, kinesisArgs)).as(LogProcessorOptions.class);

        options.setRunner(FlinkRunner.class);
        options.setAwsRegion(Regions.getCurrentRegion().getName());

        PipelineOptionsValidator.validate(LogProcessorOptions.class, options);

        // Create a PipelineOptions object. This object lets us set various execution
        // options for our pipeline, such as the runner you wish to use.
        Pipeline p = Pipeline.create(options);

        LOG.info("Running pipeline with options: {}", options.toString());

        PCollection<LogEvent> input = p
                .apply("Kinesis source", KinesisIO
                        .read()
                        .withStreamName(options.getInputStreamName())
                        .withAWSClientsProvider(new DefaultCredentialsProviderClientsProvider(Regions.fromName(options.getAwsRegion())))
                        .withInitialPositionInStream(InitialPositionInStream.LATEST)
                )
                .apply("Parse Kinesis events", ParDo.of(new EventParser.KinesisParser()));

        LOG.info("Start consuming events from stream {}", options.getInputStreamName());

        // S
        input.apply(FileIO.<Integer, LogEvent>writeDynamic()
                .by(LogEvent::getUserId)
                .to("/data")
                .withNaming(userId -> defaultNaming(userId + "-userid", ".json")));
    }
}
