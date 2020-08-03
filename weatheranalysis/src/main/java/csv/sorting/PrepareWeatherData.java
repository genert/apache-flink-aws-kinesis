package csv.sorting;

import csv.model.Station;
import csv.parser.Parsers;
import de.bytefish.jtinycsvparser.mapping.CsvMappingResult;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PrepareWeatherData {

    public static void main(String[] args) throws Exception {

        // Path to read the CSV data from:
        final Path csvStationDataFilePath = FileSystems.getDefault().getPath("/Users/genertorg/Downloads/QCLCD201712/201712station.txt");
        final Path csvLocalWeatherDataUnsortedFilePath = FileSystems.getDefault().getPath("/Users/genertorg/Downloads/QCLCD201712/201712hourly.txt");
        final Path csvLocalWeatherDataSortedFilePath = FileSystems.getDefault().getPath("/Users/genertorg/Downloads/QCLCD201712/201712hourly_sorted.txt");

        // A map between the WBAN and Station for faster Lookups:
        final Map<String, Station> stationMap = getStationMap(csvStationDataFilePath);

        // Holds the List of Sorted DateTimes (including ZoneOffset):
        List<Integer> indices = new ArrayList<>();

        // Comparator for sorting the File:
        Comparator<OffsetDateTime> byMeasurementTime = (e1, e2) -> e1.compareTo(e2);

        // Get the sorted indices from the stream of LocalWeatherData Elements:
        try (Stream<CsvMappingResult<csv.model.LocalWeatherData>> stream = getLocalWeatherData(csvLocalWeatherDataUnsortedFilePath)) {

            // Holds the current line index, when processing the input Stream:
            AtomicInteger currentIndex = new AtomicInteger(1);

            // We want to get a list of indices, which sorts the CSV file by measurement time:
            indices = stream
                    // Skip the CSV Header:
                    .skip(1)
                    // Start by enumerating ALL mapping results:
                    .map(x -> new ImmutablePair<>(currentIndex.getAndAdd(1), x))
                    // Then only take those lines, that are actually valid:
                    .filter(x -> x.getRight().isValid())
                    // Now take the parsed entity from the CsvMappingResult:
                    .map(x -> new ImmutablePair<>(x.getLeft(), x.getRight().getResult()))
                    // Take only those measurements, that are also available in the list of stations:
                    .filter(x -> stationMap.containsKey(x.getRight().getWban()))
                    // Get the OffsetDateTime from the LocalWeatherData, which includes the ZoneOffset of the Station:
                    .map(x -> {
                        // Get the matching station:
                        csv.model.Station station = stationMap.get(x.getRight().getWban());
                        // Calculate the OffsetDateTime from the given measurement:
                        OffsetDateTime measurementTime = OffsetDateTime.of(x.getRight().getDate(), x.getRight().getTime(), ZoneOffset.ofHours(0));
                        // Build the Immutable pair with the Index again:
                        return new ImmutablePair<>(x.getLeft(), measurementTime);
                    })
                    // Now sort the Measurements by their Timestamp:
                    .sorted((x, y) -> byMeasurementTime.compare(x.getRight(), y.getRight()))
                    // Take only the Index:
                    .map(x -> x.getLeft())
                    // And turn it into a List:
                    .collect(Collectors.toList());
        }

        // Now sorts the File by Line Number:
        writeSortedFileByIndices(csvLocalWeatherDataUnsortedFilePath, indices, csvLocalWeatherDataSortedFilePath);
    }


    private static void writeSortedFileByIndices(Path csvFileIn, List<Integer> indices, Path csvFileOut) {
        try {
            List<String> csvDataList = new ArrayList<>();

            // This is sorting for the dumb (like me). Read the entire CSV file, skipping the first line:
            try (Stream<String> lines = Files.lines(csvFileIn, StandardCharsets.US_ASCII).skip(1)) {
                csvDataList = lines.collect(Collectors.toList());
            }
            // Now write the sorted file:
            try (BufferedWriter writer = Files.newBufferedWriter(csvFileOut)) {
                for (Integer index : indices) {
                    writer.write(csvDataList.get(index));
                    writer.newLine();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<CsvMappingResult<csv.model.LocalWeatherData>> getLocalWeatherData(Path path) {
        return Parsers.LocalWeatherDataParser().readFromFile(path, StandardCharsets.US_ASCII);
    }

    private static Stream<csv.model.Station> getStations(Path path) {
        return Parsers.StationParser().readFromFile(path, StandardCharsets.US_ASCII)
                .filter(x -> x.isValid())
                .map(x -> x.getResult());
    }

    private static Map<String, csv.model.Station> getStationMap(Path path) {
        try (Stream<csv.model.Station> stationStream = getStations(path)) {
            return stationStream
                    // Group by WBAN, there are probably faulty entries with duplicate Station IDs:
                    .collect(Collectors.groupingBy(Station::getWban))
                    // Turn into a Stream again:
                    .entrySet().stream()
                    // Make a best guess and take the first one:
                    .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().get(0)));
        }
    }
}