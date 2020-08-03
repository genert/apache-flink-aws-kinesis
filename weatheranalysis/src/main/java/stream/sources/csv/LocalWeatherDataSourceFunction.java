package stream.sources.csv;

import csv.parser.Parsers;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import stream.sources.csv.converter.LocalWeatherDataConverter;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalWeatherDataSourceFunction implements SourceFunction<model.LocalWeatherData> {

    private volatile boolean isRunning = true;

    private String stationFilePath;
    private String localWeatherDataFilePath;

    public LocalWeatherDataSourceFunction(String stationFilePath, String localWeatherDataFilePath) {
        this.stationFilePath = stationFilePath;
        this.localWeatherDataFilePath = localWeatherDataFilePath;
    }

    @Override
    public void run(SourceFunction.SourceContext<model.LocalWeatherData> sourceContext) throws Exception {

        // The Source needs to be Serializable, so we have to construct the Paths at this point:
        final Path csvStationPath = FileSystems.getDefault().getPath(stationFilePath);
        final Path csvLocalWeatherDataPath = FileSystems.getDefault().getPath(localWeatherDataFilePath);

        // Get the Stream of LocalWeatherData Elements in the CSV File:
        try(Stream<model.LocalWeatherData> stream = getLocalWeatherData(csvStationPath, csvLocalWeatherDataPath)) {

            // We need to get an iterator, since the SourceFunction has to break out of its main loop on cancellation:
            Iterator<model.LocalWeatherData> iterator = stream.iterator();

            // Make sure to cancel, when the Source function is canceled by an external event:
            while (isRunning && iterator.hasNext()) {
                sourceContext.collect(iterator.next());
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private Stream<model.LocalWeatherData> getLocalWeatherData(Path csvStationPath, Path csvLocalWeatherDataPath) {

        // A map between the WBAN and Station for faster Lookups:
        final Map<String, csv.model.Station> stationMap = getStationMap(csvStationPath);

        // Turns the Stream of CSV data into the Elasticsearch representation:
        return getLocalWeatherData(csvLocalWeatherDataPath)
                // Only use Measurements with a Station:
                .filter(x -> stationMap.containsKey(x.getWban()))
                // And turn the Station and LocalWeatherData into the ElasticSearch representation:
                .map(x -> {
                    // First get the matching Station:
                    csv.model.Station station = stationMap.get(x.getWban());
                    // Convert to the Elastic Representation:
                    return LocalWeatherDataConverter.convert(x, station);
                });
    }

    private static Stream<csv.model.LocalWeatherData> getLocalWeatherData(Path path) {
        return Parsers.LocalWeatherDataParser().readFromFile(path, StandardCharsets.US_ASCII)
                .filter(x -> x.isValid())
                .map(x -> x.getResult());
    }

    private static Stream<csv.model.Station> getStations(Path path) {
        return Parsers.StationParser().readFromFile(path, StandardCharsets.US_ASCII)
                .filter(x -> x.isValid())
                .map(x -> x.getResult());
    }

    private Map<String, csv.model.Station> getStationMap(Path path) {
        try (Stream<csv.model.Station> stationStream = getStations(path)) {

            return stationStream
                    // Group by WBAN, there are probably faulty entries with duplicate Station IDs:
                    .collect(Collectors.groupingBy(x -> x.getWban()))
                    // Turn into a Stream again:
                    .entrySet().stream()
                    // Make a best guess and take the first one:
                    .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().get(0)));
        }
    }
}