package org.hasadna.gtfs.service;

import io.vavr.Tuple2;
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
import java.util.stream.Collectors;
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
        // TODO cache the lines of the file (especially for stop_times.txt which is a very big file (~1GB)
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
     *  Shapes.txt
     *
     ****************************/

    public Stream<String> shapeLinesFromFile(String fileFullPath) {
        io.vavr.collection.Stream<String> lines = io.vavr.collection.Stream.empty();
        try {
            lines = readZipFileV(fileFullPath, "shapes.txt")
                    .collect(io.vavr.collection.Stream.collector());
        } catch (Exception ex) {

        }
        return lines.toJavaStream();
    }

    /****************************
     *
     *  Trips.txt
     *
     ****************************/

    public Stream<String> tripLinesFromFile(String fileFullPath) {
        try {
            return readZipFile(fileFullPath, "trips.txt");
        }
        catch (Exception ex) {
            return Stream.empty();
        }
    }


    /****************************
     *
     *  Calendar.txt
     *
     ****************************/

    public Stream<String> calendarLinesFromFile(String fileFullPath) {
        try {
            return readZipFile(fileFullPath, "calendar.txt");
        }
        catch (Exception ex) {
            return Stream.empty();
        }
    }


    /****************************
     *
     *  TripIdToDate.txt
     *
     ****************************/

    public Stream<String> makatLinesFromFile(String fileFullPath) {
        Stream<String> lines = Stream.empty();
        try {
            lines = readZipFile(fileFullPath, "TripIdToDate.txt");
        } catch (Exception ex) {
            // absorb on purpose
        }
        return lines;
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

    static int counter = 0 ;

    public List<String> stopTimesLinesOfTripsFromFile(String fileFullPath, Set<String> tripIds) {
        // depending on the number of trips, this method could take over 60 seconds to complete!
        String FILE_NAME_INSIDE_GTFS_ZIP = "stop_times.txt";
        logger.info("read file {} inside {}, filter trips {} ...", FILE_NAME_INSIDE_GTFS_ZIP, fileFullPath, tripIds);
        logger.info("=== wait patiently - this could take up to 60 seconds! ===");
        Stream<String> lines = null;
        Set<String> tripsInThisFile = new HashSet<>();
        counter = 0 ;
        try {
            lines = readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
                    .filter(line -> {
                        counter = counter + 1 ;
                        if (counter % 10000000 == 0) {logger.info("file stop_times.txt: number of lines read = {}",counter);}
                        return lineBelongsToAnyTripId(tripIds, line);
                    })

                    ;
        } catch (Exception ex) {
            logger.error("exception", ex);
        }
        try {   // this is where the reading from file actually happens!
            return List.ofAll(lines);
        }
        finally {
            logger.info("                             ...Done");
        }
    }

    private boolean lineBelongsToAnyTripId(final Set<String> tripIds, final String line) {
        // this works fine, but is a bit slow
        //return tripIds.stream().anyMatch(tripId -> line.startsWith(tripId + "_"));
        // this saves a few seconds:
        for (String tripId : tripIds) {
            if (
                    line.startsWith(tripId + "_") ||
                    (line.startsWith(tripId) && tripId.contains("_"))
            ) {
                return true;
            }
        }
        // TODO check if following code improves performance:
        //String lineStart = line.split("_")[0];
        //return (tripIds.contains(lineStart));
        return false;
    }


    private boolean lineBelongsToAnyTripId2(final Set<Tuple2<String, String>> trips, final String line) {
        // this works fine, but is a bit slow
        //return tripIds.stream().anyMatch(tripId -> line.startsWith(tripId + "_"));
        // this saves a few seconds:
        for (Tuple2<String, String> trip : trips) {
            // TODO trip._2 is aimedDeparture of trip, trip._1 is tripId
//            if (line.startsWith(tripId + "_")) {
//                return true;
//            }
        }
        // TODO check if following code improves performance:
        //String lineStart = line.split("_")[0];
        //return (tripIds.contains(lineStart));
        return false;
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
                    .drop(1)    // first row in file is the headers
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