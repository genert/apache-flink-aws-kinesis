package org.genertorg;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public interface LogProcessorOptions extends FlinkPipelineOptions, AwsOptions {
    Logger LOG = LoggerFactory.getLogger(LogProcessorOptions.class);

    @Description("Name of the Kinesis Data Stream to read from")
    String getInputStreamName();

    void setInputStreamName(String value);

    static String[] argsFromKinesisApplicationProperties(String[] args, String applicationPropertiesName) {
        Properties beamProperties = null;

        try {
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

            if (applicationProperties == null) {
                LOG.warn("Unable to load application properties from the Kinesis Analytics Runtime");

                return new String[0];
            }

            beamProperties = applicationProperties.get(applicationPropertiesName);

            if (beamProperties == null) {
                LOG.warn("Unable to load {} properties from the Kinesis Analytics Runtime", applicationPropertiesName);

                return new String[0];
            }

            LOG.info("Parsing application properties: {}", applicationPropertiesName);
        } catch (IOException e) {
            LOG.warn("Failed to retrieve application properties", e);

            return new String[0];
        }

        String[] kinesisOptions = beamProperties
                .entrySet()
                .stream()
                .map(property -> String.format("--%s%s=%s",
                        Character.toLowerCase(((String) property.getKey()).charAt(0)),
                        ((String) property.getKey()).substring(1),
                        property.getValue()
                ))
                .toArray(String[]::new);

        return kinesisOptions;
    }
}
