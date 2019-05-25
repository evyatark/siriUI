package org.hasadna.gtfs.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
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

    @Value("${gtfsZipFileDirectory}")
    public String directoryOfGtfsFile;

    @Value("${tripIdToDate.ZipFileDirectory}")
    public String directoryOfMakatFile;

//    private final String GTFS_DIR = "/home/evyatar/logs/work/work 2019-04-18/gtfs2019-04-18/";
//    private final String TRIPS_FILE = GTFS_DIR + "trips.txt";
//    private final String SHAPES_FILE = GTFS_DIR + "shapes.txt";

    @Cacheable("shapeByRouteAndDate")
    public String findShape(String routeId, String date) {
        try {

            ReadZipFile rzf = new ReadZipFile();
            String gtfsZipFileName = "gtfs" + date + ".zip";
            String gtfsZipFileFullPath = directoryOfGtfsFile + File.separatorChar + gtfsZipFileName;

            //read trips file, find line that contains routeId
            // you get 2 lines but in both of them it is the same shape, so take the first.
            // from that line take 4th item (?) - this is the shape id
            //  readFromLog(TRIPS_FILE)
            Optional<String> tripLine = rzf
                    .tripLinesFromFile(gtfsZipFileFullPath)
                    .filter(line -> line.startsWith(routeId + ","))
                    .findFirst();

            logger.info("routeId={}, tripLine={}", routeId, tripLine.orElse("not found"));
            // route_id,service_id,trip_id,trip_headsign,direction_id,shape_id

            Optional<String> shapeIdOpt = tripLine
                    .map(line -> line.split(","))   // split the line by commas, if tripLine has a value
                    .map(arr -> arr[5]);                  // takes the 6th item from the array (if tripLine had a value)
            if (!shapeIdOpt.isPresent()) {
                logger.warn("could not find shapeId for route {}", routeId);
                return "[]";
            }
            //tripLine.map(line -> line.split(",")).map(arr -> arr[5]).orElse("");
            String shapeId = shapeIdOpt.get();
            List<String> shapeLines =
                    rzf.shapeLinesFromFile(gtfsZipFileFullPath)
                            .filter(line -> line.startsWith(shapeId))
                            .collect(Collectors.toList()); // only the lines that start with shapeId as the first item
            logger.info("collected {} lines for shape {}", shapeLines.size(), shapeId);
            String json = generateShapeJson(shapeLines);
            logger.info("json for shape {}: {}", shapeId, json.substring(0, 2000));
            // [32.054065,35.239214][32.054393,35.239772][32.054735,35.240334]
            return json;
        }
        catch (Exception ex) {
            logger.error("during findShape, for routeId={}", routeId, ex);
            return "[]";
        }
    }

    private String generateShapeJson(List<String> shapeLines) {
        String shapePoints = "" + shapeLines.stream()
                .map(line -> "[" + line.split(",")[1] + "," + line.split(",")[2] + "]")
                .reduce((a, b) -> a + "," + b).get();
        return "[" +  shapePoints + "]";
    }


}
