package org.hasadna.gtfs.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import org.hasadna.gtfs.entity.StopsTimeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class SiriData {

    private static Logger logger = LoggerFactory.getLogger(SiriData.class);

    @Value("${gtfsZipFileFullPath:/home/evyatar/logs/work/2019-04/gtfs/}")
    public String gtfsZipFileDirFullPath = "";

    @Value("${search.stops.in.gtfs:false}")
    public boolean searchGTFS ;

    @Autowired
    Stops stops;

    /**
     * create a Stream of lines from several gz siri results files
     *
     * @param fileNames
     * @return
     */
    public Stream<String> readSeveralGzipFiles(String... fileNames) {
        return List.of(fileNames)
                .map(fileName ->readSiriGzipFile(fileName))
                .reduce((s1, s2) -> Stream.concat(s1, s2) ) ;
    }

    /**
     * Read a GZip file of Siri results and return a lazy stream of lines
     * path would be like /home/evyatar/logs/data/siri_rt_data.2019-04-04.8.log.gz
     * @param path
     * @return
     */
    public Stream<String> readSiriGzipFile(String path) {
        Path p = Paths.get(path);
        boolean ok = Files.isRegularFile(p) ;
        if (!ok) return Stream.empty();
        Stream<String> lines = GZIPFiles.lines(p);
        return lines;
    }

    /**
     * Group all lines in the stream, group by the routeId.
     * Also filter out empty lines.
     * @param lines
     */
    public void groupByRoute(Stream<String> lines) {

        io.vavr.collection.Stream <String> tuples =
                lines
                        .filter(line -> line.length() > 1)
                        .collect(io.vavr.collection.Stream.collector());

        Map<String, io.vavr.collection.Stream<String> > routes = tuples.groupBy(line -> extractRouteId(line));
        // in routes, key is routeId, and value is all lines of buses doing that route (could be from different trips)
        routes.keySet().forEach(key -> logger.info("routeId={}, lines={}", key, routes.get(key).get().count(t -> true)));
   }

    public String extractRouteId(String line) {
        return line.split(",")[3];
    }

    public String extractTripId(String line) {
        return line.split(",")[5];
    }

    public String extractShortName(String line) {
        return line.split(",")[4];
    }

    public String extractAimedDeparture(String line) {
        return line.split(",")[6];
    }

    public String extractAgency(String line) {
        return line.split(",")[2];
    }

    public String extractSiriDescription(String line) {
        return line.split(",")[1];
    }

    private String extractGpsTimestamp(String line) {
        return line.split(",")[9];
    }

    private String extractTimestamp(String line) {
        return line.split(",")[0];
    }

    private String extractRecalculatedETA(String line) {
        return line.split(",")[8];
    }

    private String extractVehicleId(String line) {
        return line.split(",")[7];
    }

    private String[] extractGpsLatLong(String line) {
        String lat = line.split(",")[10];
        String longitude = line.split(",")[11];
        return new String[]{lat, longitude};
    }

    /**
     * Create JSON representation of Siri results for the specified routeId and Date
     * @param routeId
     * @param date
     *
     * resulting JSON will look like this:
     *
     *
        [                   // array of objects
            {               // each object contains some properties (routeId, etc)
                            // and a property named "siri1" which is an array of Siri objects
                            // (gps location with a timestamp)
                "routeId": "15531",
                "shortName": "420",
                "agencyCode": "16",
                "agencyName": null,
                "dayOfWeek": "MONDAY",
                "date": "2019-03-04",
                "originalAimedDeparture": null,
                "gtfsETA": null,
                "gtfsTripId": null,
                "stopsTimeData": null,
                "siriTripId": "36619535",
                "vehicleId": "3175078",
                "siri1": [
                    {
                        "timestamp": "2019-03-04T05:20:08.073",
                        "timestampGPS": "05:20:02",
                        "latLong": ["31.74322509765625", "34.98543930053711"],
                        "recalculatedETA": null
                    },
                    ...
                ]
            },
            ...
        ]
     */
    @Cacheable("default")
    public String dayResults(final String routeId, String date) {
        logger.warn("day results: routeId={}, date={}", routeId, date);
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
        final String ROUTE_ID = routeId;
        //ist<String> names = List.range(0, 1).map(i -> "/home/evyatar/logs/data/siri_rt_data." + date + "." + i + ".log.gz");  // 2019-04-04
        List<String> names = List.range(0, 20).map(i -> "/home/evyatar/logs/data/siri_rt_data." + date + "." + i + ".log.gz");  // 2019-04-04

        logger.warn("reading {} siri results log files", names.size());

        Stream<String> lines = this
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> !line.endsWith(",0,0"))

                //.limit(70000)

                .filter(line -> ROUTE_ID.equals(this.extractRouteId(line)));

        logger.info("completed reading {} siri results log files", names.size());

        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());

        logger.info("grouping by tripId");

        Map<String, io.vavr.collection.Stream<String>> trips = vLines.groupBy(line -> this.extractTripId(line));
        logger.warn("{} {} route {}:got {} trips, {} of them suspicious", dayOfWeek, date, routeId, trips.size(), trips.count(trip->trip._2.count(i->true)<28));
        String json = "";
        try {
            logger.info("building json for all trips...");
            json = buildJson(trips, dayOfWeek, date, routeId);
            // TODO call createReport(trips, dayOfWeek, date, routeId);
        } catch (JsonProcessingException e) {
            logger.error("", e);
        }
        logger.warn(json);
        return json;
    }

    /**
     * build JSON representation of Siri results from the data in the method arguments
     * @param trips
     * @param dayOfWeek
     * @param date
     * @param routeId
     * @return
     * @throws JsonProcessingException
     */
    private String buildJson(Map<String,io.vavr.collection.Stream<String>> trips, DayOfWeek dayOfWeek, String date, String routeId) throws JsonProcessingException {
        //boolean searchGTFS = false;
        java.util.List<TripData> tripsData = buildTripData(trips, dayOfWeek, date, routeId);
        List<String> suspicious =
                List.ofAll(
                        tripsData.stream()
                                .filter(trip -> trip.suspicious.equals("true"))
                                .map(tripData -> tripData.siriTripId) //using vavr instead of Java List
                );
        suspicious.forEach(tripId -> {
                            int numberOfSiriPoints = List.ofAll(tripsData.stream())
                                    .filter(tripData -> tripData.siriTripId.equals(tripId))
                                    .map(td -> td.siri.features.length).get(0);
                            logger.info("trip {} is suspicious: has only {} GPS points", tripId, numberOfSiriPoints);
                        });
        // without the GTFS reading, we have a nice JSON, we only miss trips that were planned but not executed at all!
        // for example 420 2019-03-31 8:20
        // To know times of all planned trips, we can use the schedule files - in this case "siri.schedule.16.Sunday.json.2019-03-31"
        // (or we can read GTFS files and calculate again. Which is a small addition to
        // reading the time consuming Stops data and times that we can do now if searchGTFS is true)

        // tripsData is trips that we found in Siri. But sometimes these trip IDs are not found in GTFS???
        if (searchGTFS) {   // this block searches in GTFS files, which is very time consuming!
            // GTFS data is needed for displaying the stops in Leaflet widget UI
            logger.info("reading data about stops, from GTFS file ...");
            Set<String> tripIds = findAllTripIds(tripsData);
            java.util.Map<String, java.util.Map<Integer, StopsTimeData>> map = new HashMap<>();
            java.util.Map<String, java.util.Map<Integer, StopsTimeData>> all =
                    stops.generateStopsMap(tripIds, date, gtfsZipFileDirFullPath, map);

            tripsData.forEach(tripData -> tripData.stopsTimeData = all.get(tripData.siriTripId));
            logger.info("                  ... Done");
        }
        logger.info("converting to JSON...");
        ObjectMapper x = new ObjectMapper();
        String json = x.writeValueAsString(tripsData);
        logger.info("                  ... Done");
        return json;
    }

    private Set<String> findAllTripIds(java.util.List<TripData> tripsData) {
        return tripsData.stream().map(tripData -> tripData.siriTripId).collect(Collectors.toSet());
    }

    /**
     * build POJO representation of trips from the data in the method arguments
     * @param trips
     * @param dayOfWeek
     * @param date
     * @param routeId
     * @return
     */
    // sample of a line:
    // 2019-04-04T17:25:12.187,[line 358 v 8335053 oad 15:20 ea 18:17],15,8136,358,37350079,15:20,8335053,18:17,17:24:47,31.802471160888672,34.83134460449219
    private java.util.List<TripData> buildTripData(Map<String, io.vavr.collection.Stream<String>> trips, DayOfWeek dayOfWeek, String date, String routeId) {
        return
            trips.keySet().map(tripId ->
                                {
                                    io.vavr.collection.Stream<String> thisTrip = trips.get(tripId).get();
                                    String firstLine = thisTrip.head();
                                    TripData td = new TripData();
                                    td.routeId = routeId;
                                    td.date = date;
                                    td.dayOfWeek = dayOfWeek.toString();
                                    td.siriTripId = tripId;
                                    //td.siri1 = thisTrip.map(line -> createSiriReading(line)).toJavaList();
                                    td.siri = new SiriFeatureCollection(thisTrip.map(line -> createSiriPart(line)).asJava().toArray(new SiriFeature[]{}));
                                    td.vehicleId = extractVehicleId(firstLine);
                                    td.agencyCode = extractAgency(firstLine);
                                    td.agencyName = agencyNameFromCode(td.agencyCode);
                                    td.shortName = extractShortName(firstLine);
                                    td.originalAimedDeparture = extractAimedDeparture(firstLine);
                                    td.suspicious = false;
                                    if (td.siri.features.length < 20) {td.suspicious = true;};
                                    return td;
                                }).toJavaList();
    }



    public java.util.List<String> findAllBusLines(String date) {
        List<String> names = List.range(0, 20).map(i -> "/home/evyatar/logs/data/siri_rt_data." + date + "." + i + ".log.gz");  // 2019-03-31

        logger.warn("reading {} siri results log files for date {}", names.size(), date);

        // calc set of routeIds that appeared in Siri results on that day
        Stream<String> routes = this
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> !line.endsWith(",0,0"))
                .map(line -> extractRouteId(line))
                .distinct();
        java.util.List<String> routeIds = routes.collect(Collectors.toList());
        return routeIds;
    }

//    private String extractFromLine(String line) {
//        extractRouteId(line);
//        //extractShortName(line);
//        //agencyNames.getOrElse(extractAgency(line), "unknown");
//        ext
//    }

    static Map<String, String> agencyNames = io.vavr.collection.HashMap.of(
                "16", "Superbus",
                "3", "Egged",
                "5", "Dan",
                "18", "Kavim",
                "15", "Metropolin",
                "25", "Afikim",
                "4", "EggedTaabura",
                "31", "DanBadarom",
                "14", "Nativ Express",
                "32", "DanBeerSheva").put("30", "DanBatzafon").put("2", "Trains");

    private String agencyNameFromCode(String agencyCode) {
        return agencyNames.getOrElse(agencyCode, agencyCode);
    }

    /**
     * create POJO representation of a single Siri reading, from a line in the siri results file
     * @param line
     * @return
     */
    private SiriReading createSiriReading(String line) {
        SiriReading sr = new SiriReading();
        sr.timestamp = extractTimestamp(line);
        sr.timestampGPS = extractGpsTimestamp(line);
        sr.latLong = extractGpsLatLong(line);
        return sr;
    }

    // 2019-04-04T17:25:12.187,[line 358 v 8335053 oad 15:20 ea 18:17],15,8136,358,37350079,15:20,8335053,18:17,17:24:47,31.802471160888672,34.83134460449219

    private SiriFeature createSiriPart(String line) {
        SiriFeature sf = new SiriFeature();
        sf.geometry = new PointGeometry();
        sf.geometry.coordinates = extractGpsLatLong(line);
        sf.properties = new SiriProperties();
        sf.properties.time_recorded = extractGpsTimestamp(line);
        sf.properties.timestamp = extractTimestamp(line);
        sf.properties.recalculatedETA = extractRecalculatedETA(line);
        return sf;
    }


    /*
    {
        route
        shortName
        agencyCode
        agencyName
        date
        originalAimedDeparture
        gtfsEta
        gtfsTripId
        siriTripId
        vehicleId
        gps: [
            {
                timestamp
                timestamp_gps
                lat,long
                recalculated_eta
        ]

    }
    */
}


