package org.hasadna.gtfs.service;

import io.vavr.collection.List;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ReadZipFile {

    private static Logger logger = LoggerFactory.getLogger(ReadZipFile.class);

    public io.vavr.collection.Stream<String> readZipFileV(String filePath, String fileInsideZip) throws IOException {
        io.vavr.collection.Stream <String> vLines =
            readZipFile(filePath, fileInsideZip)
                    .collect(io.vavr.collection.Stream.collector());
        return vLines;
    }

    public Stream<String> readZipFile(String filePath, String fileInsideZip) throws IOException {
        final ZipFile file = new ZipFile(filePath);
        ZipEntry entry = findEntry(file.entries(), fileInsideZip);
        if (entry == null) return null;
        return readFromInputStream(file.getInputStream(entry));
    }


    public ZipEntry findEntry(Enumeration<? extends ZipEntry> entries, String fileInsideZip) {
        while (entries.hasMoreElements()) {
            final ZipEntry entry = entries.nextElement();
            if (entry.getName().equals(fileInsideZip)) {
                return entry;
            }
        }
        return null;
    }

    private Stream<String> readFromInputStream(InputStream is) {
        InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);
        return br.lines();
    }


    /****************************
     *
     *  Stops.txt
     *
     ****************************/

    public io.vavr.collection.Stream<String> stopLinesFromFile(String fileFullPath) {
        io.vavr.collection.Stream<String> lines = io.vavr.collection.Stream.empty();
        try {
            lines = readZipFileV(fileFullPath, "stops.txt")
                    .collect(io.vavr.collection.Stream.collector());
        } catch (Exception ex) {

        }
        return lines;
    }


    /****************************
     *
     *  Stop_Times.txt
     *
     ****************************/

    /**
     * return a (lazy!) Stream of all lines in stop_times.txt file inside the GTFS zip file.
     * client code using this method is expected to filter only a small portion of the lines
     * (for eaxmple, all lines of a specific trip id)
     * @param fileFullPath
     * @return
     */
    public io.vavr.collection.Stream<String> stopTimesLinesFromFile(String fileFullPath) {
        io.vavr.collection.Stream<String> lines = null;
        try {
            lines = readZipFileV(fileFullPath, "stop_times.txt")
                    .collect(io.vavr.collection.Stream.collector());
        } catch (Exception ex) {

        }
        return lines;
    }

    public io.vavr.collection.Stream<String> stopTimesLinesOfTripFromFile(String fileFullPath, String tripId) {
        io.vavr.collection.Stream<String> lines = null;
        try {
            lines = readZipFileV(fileFullPath, "stop_times.txt")
                    .filter(line -> line.startsWith(tripId + "_"))
                    .collect(io.vavr.collection.Stream.collector());
        } catch (Exception ex) {

        }
        return lines;
    }

    public Stream<String> stopTimesLinesOfTripFromFile1(String fileFullPath, String tripId) {
        String FILE_NAME_INSIDE_GTFS_ZIP = "stop_times.txt";
        logger.info("read file {} inside {}, filter trip {} ...", FILE_NAME_INSIDE_GTFS_ZIP, fileFullPath, tripId);
        Stream<String> lines = null;
        Set<String> tripsInThisFile = new HashSet<>();
        try {
            lines = readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
                    .filter(line -> {
                        String trip = line.split(",")[0];
                        if (!tripsInThisFile.contains(trip)) {
                            tripsInThisFile.add(trip);
                            logger.debug(trip);
                        }
                        if (line.startsWith(tripId.substring(0,6)))
                            logger.debug("{}: {}", tripId, line);
                        return line.startsWith(tripId + "_");
                    });
        } catch (Exception ex) {

        }
        logger.info("                             ...Done");
        logger.info("trips in this stop_times file: {}", tripsInThisFile);
        return lines;
    }


    /****************************
     *
     *  Routes.txt
     *
     ****************************/

    public io.vavr.collection.Stream<String> routesFromFile(String fileFullPath) {
        io.vavr.collection.Stream<String> lines = io.vavr.collection.Stream.empty();
        try {
            lines = readZipFileV(fileFullPath, "routes.txt")
                    .collect(io.vavr.collection.Stream.collector());
        } catch (Exception ex) {

        }
        return lines;
    }

    // TODO maybe return a stream, not a list?
    public List<RouteData> collectRoutes(List<String> onlyTheseRoutes, String gtfsZipFileFullPath) {
        return routesFromFile(gtfsZipFileFullPath)
                .filter(line -> onlyTheseRoutes.contains(extractRouteId(line)))
                .map(line -> new RouteData(extractRouteId(line),extractAgencyCode(line), extractShortName(line), extractFrom(line), extractDestination(line)))
                .toList();
    }

    // extracts from lines of routeId
    private String extractRouteId(String line) {
        if (StringUtils.isEmpty(line)) {
            return "";
        }
        // route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_color
        return line.split(",")[0];
    }

    private String extractAgencyCode(String line) {
        if (StringUtils.isEmpty(line)) {
            return "";
        }
        // route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_color
        return line.split(",")[1];
    }

    private String extractShortName(String line) {
        if (StringUtils.isEmpty(line)) {
            return "";
        }
        // route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_color
        return line.split(",")[2];
    }

    private String extractFrom(String line) {
        return "unknown";
    }

    private String extractDestination(String line) {
        return "unknown";
    }


    public class RouteData {
        public String routeId;
        public String agencyCode;
        public String shortName;
        public String from;
        public String to;

        public RouteData(String routeId, String agencyCode, String shortName, String from, String to) {
            this.routeId = routeId;
            this.agencyCode = agencyCode;
            this.shortName = shortName;
            this.from = from;
            this.to = to;
        }
    }
}