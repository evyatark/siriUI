package org.hasadna.gtfs.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class Shapes {

    private static Logger logger = LoggerFactory.getLogger(Shapes.class);

    private final String GTFS_DIR = "/home/evyatar/logs/work/work 2019-01-27/gtfs2019-01-27/";
    private final String TRIPS_FILE = GTFS_DIR + "trips.txt";
    private final String SHAPES_FILE = GTFS_DIR + "shapes.txt";

    @Cacheable("default")
    public String findShape(String routeId) {
        try {

            //read trips file, find line that contains routeId
            // you get 2 lines but in both of them it is the same shape, so take the first.
            // from that line take 4th item (?) - this is the shape id
            Optional<String> tripLine = readFromLog(TRIPS_FILE).filter(line -> line.startsWith(routeId + ",")).findFirst();

            logger.info("routeId={}, tripLine={}", routeId, tripLine.orElse("not found"));
            // route_id,service_id,trip_id,trip_headsign,direction_id,shape_id

            String shapeId = tripLine.map(line -> line.split(",")).get()[5];
            //tripLine.map(line -> line.split(",")).map(arr -> arr[5]).orElse("");

            List<String> shapeLines =
                    readShapesFile(shapeId)
                            .collect(Collectors.toList()); // only the lines that start with shapeId as the first item

            String json = generateShapeJson(shapeLines);
// [32.054065,35.239214][32.054393,35.239772][32.054735,35.240334]
            return json;
        }
        catch (Exception ex) {
            logger.error("during findShape, for routeId={}", routeId, ex);
            return "Server Error";
        }
    }

    private String generateShapeJson(List<String> shapeLines) {
        String shapePoints = shapeLines.stream()
                .map(line -> "[" + line.split(",")[1] + "," + line.split(",")[2] + "]")
                //.reduce(String::concat).get();
                .reduce((a, b) -> a + "," + b).get();
        return "[" +  shapePoints + "]";
    }

    private Stream<String> readShapesFile(String shapeId) throws IOException {
        return readFromLog(SHAPES_FILE).filter(line -> line.startsWith(shapeId));
    }

    public Stream<String> readFromLog(String fullFileName) throws IOException {
        Stream<String> filesList = Stream.of(fullFileName);
        return filesList.flatMap(fileName -> {
                    BufferedReader br = null;
                    try {
                        br = Files.newBufferedReader(Paths.get(fileName));
                        Stream<String> lines = br.lines();
                        return lines;
                    } catch (IOException e) {
                        //logger.error("can't read file " + fileName);
                        return Stream.empty();
                    }
                }
        );
    }
}
