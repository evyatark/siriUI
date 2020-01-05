package org.hasadna.gtfs.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.*;
import io.vavr.control.Option;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.hasadna.gtfs.Spark;
import org.hasadna.gtfs.db.MemoryDB;
import org.hasadna.gtfs.entity.InsideData;
import org.hasadna.gtfs.entity.StopsTimeData;
import org.hasadna.gtfs.repository.InsideDataRepository;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.linearref.LengthIndexedLine;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hasadna.gtfs.service.Stops.decideGtfsFileName;
import static org.hasadna.gtfs.service.Utils.generateKey;

@Component
public class SiriData {

    private static final double RADIUS_OF_EARTH = 6378137.0; // in meters
    private static Logger logger = LoggerFactory.getLogger(SiriData.class);

    /**
     * This is the directory with all GTFS files of a specific month.
     * (it is also possible to put all GTFS files of several/all months in the same directory)
     */
    // not needed
//    @Value("${gtfsZipFileDirectory}")
//    public String gtfsZipFileDirFullPath1 = "";          // :/home/evyatar/logs/work/2019-04/gtfs/

    @Value("${siriGzFilesDirectory}")
    public String siriLogFilesDirectory;    // /home/evyatar/logs/data/

    @Value("${tripIdToDate.ZipFileDirectory}")
    public String directoryOfMakatFile;

    @Value("${gtfsZipFileDirectory}")
    public String directoryOfGtfsFile;

    @Value("${enable.gtfs.stops:true}")
    public boolean displayStops;

    @Value("${search.stops.in.gtfs:false}")
    public boolean searchGTFS ;

    public boolean enrichWithDistance = true;  // true - to use geoTools Linear Reference to calculate distance

    @Autowired
    Stops stops;

    @Autowired
    SchedulesData schedulesData;

    @Autowired
    MemoryDB db;

    @Autowired
    Shapes shapeService;

    @Autowired
    ReadSiriRawData readSiriRawData;

    @Autowired
    TripFileReader tripFileReader;

    /******************* This is for calculating distance ********
     *
     */
    static final MathTransform transformer = computeOnce();

    private static MathTransform computeOnce() {
        try {
            return CRS.findMathTransform(CRS.decode("EPSG:4326", false), CRS.decode("EPSG:2039", false), true);
        }
        catch (Exception ex) {
            return null;
        }
    }

    public static Coordinate convertLatLonToITM(Coordinate coordinate) {   //throws FactoryException, MismatchedDimensionException, TransformException {
        Point point = (new GeometryFactory()).createPoint( coordinate);
        try {
            Geometry g = JTS.transform(point, transformer);
            return g.getCoordinate();
        }
        catch (Exception ex) {
            return null;
        }
    }
    /*
     *
     *********************************************************************/




    /**
     * create a Stream of lines from several gz siri results files
     *
     * @param fileNames
     * @return
     */
    public Stream<String> readSeveralGzipFiles(String... fileNames) {
//        final String key = Arrays.stream(fileNames).collect(Collectors.joining());
//        String fromDB = db.readKey(key);
//        if (fromDB != null) {
//            return Arrays.stream(fromDB.split("\n"));
//        }
        if (fileNames.length == 0) {
            return Stream.empty();
        }
        Stream<String> result = List.of(fileNames)
                .map(fileName ->readSiriGzipFile(fileName))
                .reduce((s1, s2) -> Stream.concat(s1, s2) );
//        String value = result.collect(Collectors.joining("\n"));
//        db.writeKeyValue(key, value);
        return result;
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

    public Set<String> listOfRoutes = HashSet.empty();
    public String extractRouteId(String line) {
        String routeId = line.split(",")[3];
        if (logger.isTraceEnabled()) logger.trace("line for route {}", routeId);
        listOfRoutes = listOfRoutes.add(routeId);
        return routeId;
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
    @Autowired
    Spark spark;

    //@Cacheable("siriByRouteAndDay")
    public String dayResults(final String routeId, final String date, final boolean withReadingSiriLogs) {

        String json;
        if (withReadingSiriLogs) {
            json = dayResultsWithSiri(routeId, date);
        }
        else {
            // without siri logs
            final String key = generateKey("gtfs", routeId, date);
            String fromDB = db.readKey(key);
            if ((fromDB != null) && !"[]".equals(fromDB)) {
                json = fromDB;
            }
            else {
                logger.debug("skip Siri logs, read only GTFS");
                final List<TripData> tripsData = buildFullTripsDataWithoutSiri(date, routeId);
                json = convertToJson(tripsData);
                if (tripsData != null) db.writeKeyValue(key, json);
            }
        }
        logger.debug("day results completed: routeId={}, date={}", routeId, date);
        return json;
    }

    private String dayResultsWithSiri(final String routeId, final String date) {
        final String key = generateKey("siri", routeId, date);
        final String fromDB = db.readKey(key);
        if ((fromDB != null) && !"[]".equals(fromDB)) return fromDB;
        logger.debug("read Siri logs, then read also GTFS (for data about stops)");

        final List<TripData> tripsData = dayResultsWithSiri1(routeId, date);

        logger.debug("converting result to json...");

        final String json = convertToJson(tripsData);

        logger.trace("json result: {}", json.length() > 100 ? json.substring(0, 100) : json);
        logger.debug("writing result to inMemoryDB with key {}", key);

        if (tripsData != null) db.writeKeyValue(key, json);
        return json;
    }

    private List<TripData> dayResultsWithSiri1(final String routeId, final String date) {
        // SIRI

        // this reads all siri logs (of that day) to find text lines of that route
        // map of all trips of the specified routeId at day {date}
        // key: tripId
        // value: Stream/List of all lines from the siri_rt-data file(s), that belong to this trip
        final Map<String, io.vavr.collection.Stream<String>> trips = findAllTrips(routeId, date);
        if (trips.isEmpty()) return List.empty();

        // GTFS
        List<TripData> tripsData = buildFullTripsData2(trips, date, routeId);

        // geographic (distances)
        if (enrichWithDistance) {
            logger.info("enriching tripsData {}, {} with geographic data", routeId, date);
            tripsData = enrich(tripsData, routeId, date);
        }

        return tripsData;
    }

    Coordinate fromFeature(PointGeometry pg) {
        return  new Coordinate(
            Double.parseDouble(pg.coordinates[0]),
                Double.parseDouble(pg.coordinates[1]));
    }

    private List<TripData> enrich(List<TripData> tripsData, final String routeId, final String date) {
        try {
            LengthIndexedLine indexedLine = buildIndexedLine(routeId, date);
            // start the indexed line from the first stop (not from start of shape linestring!!)
            double start = indexedLine.getStartIndex();
            Coordinate startOfLine = indexedLine.extractPoint(start);   // this is beginning of Shape. But sometimes it is NOT the location of first stop!!
            // since location of stops is the same all day, it is tempting to say that it is enough to take one trip
            // but some trips might have stops == null.
            TripData theChosen = findTripWithStops(tripsData);
            StopFeature[] allStopsOnRoute = theChosen.stops.features;
            StopFeature firstDepartureStop = List.of(allStopsOnRoute).find(stop -> "1".equals(stop.properties.stop_sequence)).getOrNull();
            if (firstDepartureStop != null) {
                logger.info("adjusting start of route to first stop...");
                Coordinate locationOfFirstDepartureStopInGeographicLatLon = fromFeature(firstDepartureStop.geometry);
                Coordinate utmCoord = convertLatLonToITM(locationOfFirstDepartureStopInGeographicLatLon);
                startOfLine = utmCoord;
                double indexOfFirstDepartureStop = indexedLine.project(startOfLine);
                indexedLine = new LengthIndexedLine( indexedLine.extractLine(indexOfFirstDepartureStop, indexedLine.getEndIndex()) );
                start = indexedLine.getStartIndex();
                logger.info("... adjusted");
            }
            for (TripData tripData : tripsData) {
                if (tripData.siri == null) continue;
                logger.debug("enriching trip {} that has {} siri features", tripData.siriTripId, tripData.siri.features.length);

                for (SiriFeature sf : tripData.siri.features) {
                    Coordinate siriLocation = new Coordinate(Double.parseDouble(sf.geometry.coordinates[0]), Double.parseDouble(sf.geometry.coordinates[1]));
                    Coordinate utmCoordinate = convertLatLonToITM(siriLocation);
                    double indexOfSiriPoint = indexedLine.project(utmCoordinate);
                    long distanceInMeters = new Double(indexedLine.extractLine(start, indexOfSiriPoint).getLength()).longValue();
                    logger.trace("added distance {} to siriFeature {}", distanceInMeters, sf.properties.time_recorded);
                    sf.properties.distanceFromStart = distanceInMeters;
                }
            }
        }
        catch (Exception ex) {
            logger.info("failed to enrich (with distance) Siri Locations in trips. Absorbing exception", ex);
        }
        return tripsData;
    }

    private TripData findTripWithStops(final List<TripData> tripsData) {
        final TripData NULL_TRIP = null;
        final Option<TripData> tripOption = tripsData.find(tripData -> tripData.stops != null);
        return tripOption.getOrElse(NULL_TRIP);
    }

    private LengthIndexedLine buildIndexedLine(final String routeId, final String date) {
        try {
            String shape = shapeService.findShape(routeId, date);
            if (shape == null) {
                logger.error("no shape for route {} date {}, can't build LengthIndexedLine from shape", routeId, date);
                return null;
            }
            String shapeWkt = convertToUtmAndConstructWktFromShape(shape);
            //String shapeWkt = "LINESTRING ( 198307.16448649915 627976.016711362, 198320.8974821051 627972.7729288177, ...)\n";
            logger.debug("shape wkt= {}", shapeWkt);
            GeometryFactory fact = new GeometryFactory();
            WKTReader rdr = new WKTReader(fact);
            Geometry g = rdr.read(shapeWkt);
            LengthIndexedLine indexedLine = new LengthIndexedLine(g);
            return indexedLine;
        }
        catch (Exception ex) {
            logger.error("failed to build LengthIndexedLine from shape", ex);
            return null;
        }
    }

    private String convertToUtmAndConstructWktFromShape(String shape) throws ParseException {
        String coordsInShape = shape.split("\"shape\":")[1];
        logger.info("{}", coordsInShape);
        String json =
                "{\"type\": \"LineString\",\"coordinates\": " +
                        shape.split("\"shape\":")[1];
        Geometry geometry = (new GeoJsonReader()).read(json);
        Coordinate[] coords = geometry.getCoordinates();
        List<Coordinate> utmCoords = List.of(coords).map(coordinate -> new Coordinate(convertLatLonToITM(coordinate)));
        String coordsAsStr = utmCoords.map(coord -> " " + coord.getX() + " " + coord.getY()).collect(Collectors.joining(","));
        String lineString = "LINESTRING (" + coordsAsStr + ")";
        return lineString;
    }

    private TripData BuidTripDataForMissingDeparture(String departure, String date, String routeId) {
        TripData tripData = new TripData();
        tripData.originalAimedDeparture = departure;
        tripData.suspicious = true;
        tripData.date = date;
        tripData.routeId = routeId;
        return tripData;
    }


    public Map<String, Map<String, Stream<String>>> groupLinesOfEachRoute(final List<String> allRouteIds, final String date) {
        // find in redis
        List<String> exist = List.empty();
        for (String routeId : allRouteIds) {
            java.util.Map<String, java.util.List<String>> res = null ;
            try {
                //res = jsonClient.getSiriResultsOfRoute(date, routeId);
            }
            catch (Exception ex) {
                // absorb, res will be null
            }
            if ((res == null) || res.isEmpty()) {
                continue;
            }
            else {
                exist = exist.append(routeId);
            }
        }
        logger.info("for these routes {} already have siri results in Redis", exist);
        final List<String> exist1 = exist;//List.ofAll(exist);
        List<String> remain = allRouteIds.filter(routeId -> !exist1.contains(routeId)).sorted();
//        if (remain.isEmpty()) {
        // existing in Redis
        logger.info("some routeIds found in Redis, retrieve...");
        Map<String, Map<String, Stream<String>>> mapRouteAndTrip1 = HashMap.empty();
        for (String routeId : exist) {
//            mapRouteAndTrip1 = mapRouteAndTrip1.put(routeId,
//                                                convert(jsonClient.getSiriResultsOfRoute(date, routeId)));

            logger.info("retreived {}", mapRouteAndTrip1.keySet().toList());
            //return mapRouteAndTrip1;
        }

        if (remain.isEmpty()) {
            logger.info("all of them found in Redis, so return them");
            return mapRouteAndTrip1;
        }

        // else
        logger.info("scanning siri logs for these routes: {}", remain);

        final List<String> routeIds = remain;
        // else - need to find in siri results log files these route IDs

        // names: list of names of all siri_rt_data files from the specified date
        // (assumes we won't have more than 20 files of siri results in the same date)        List<String> names1 = findFileNames();
        String fileName = "siri_rt_data_v2." + date + "." + 0 + ".log.gz";
        String fullPath = Utils.findFile(siriLogFilesDirectory, fileName);
        if (fullPath == null) {
            logger.warn("could not find file {} in path {}", fileName, siriLogFilesDirectory);
        } else {
            logger.warn("found file {} in path {}, full path is {}", fileName, siriLogFilesDirectory, fullPath);
        }
        List<String> namesOldFormat = List.range(0, 20).map(i -> Utils.findFile(siriLogFilesDirectory, "siri_rt_data." + date + "." + i + ".log.gz")).filter(s -> s != null);  // 2019-04-04
        List<String> names = List.range(0, 20).map(i -> Utils.findFile(siriLogFilesDirectory, "siri_rt_data_v2." + date + "." + i + ".log.gz")).filter(s -> s != null);  // 2019-04-04
        names = names.appendAll(namesOldFormat);
        logger.debug("the files are: {}", names.toString());
        logger.info("reading {} siri results log files...", names.size());

        // lines/vLines: Stream/List of all lines from the siri_rt-data file(s) [of day {date}, that belong to route ROUTE_ID
        logger.debug("looking for routeIds: {}", routeIds.toJavaList().toString());
        Stream<String> lines = this
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> gpsExists(line))
                .filter(line -> routeIds.contains(this.extractRouteId(line)));
        logger.trace("after filtering, remained with {} text lines", lines.toJavaList().size());
        listOfRoutes.dropWhile(value -> value != null);
        Map<String, Stream<String>> mapRouteIdToLinesOfSiriLog = lines.groupBy(line -> extractRouteId(line));
        logger.trace("mapOfLines has {} keys. List of routes has {}", mapRouteIdToLinesOfSiriLog.keySet().toJavaList().size(), listOfRoutes.size());
        //listOfRoutes.forEach(routeId -> logger.debug("routeId={}", routeId));
        Map<String, Map<String, Stream<String>>> mapRouteAndTrip = HashMap.empty();
        for (String routeId : mapRouteIdToLinesOfSiriLog.keySet()) {
            Stream<String> linesOfRoute = mapRouteIdToLinesOfSiriLog.getOrElse(routeId, Stream.empty());
            Map<String, Stream<String>> mapTripIdToLinesOfSiriLog = linesOfRoute.groupBy(line -> extractTripId(line));
            mapRouteAndTrip = mapRouteAndTrip.put(routeId, mapTripIdToLinesOfSiriLog);
        }
        final List<String> foundInSiri = mapRouteAndTrip.keySet().toList().sorted();
        logger.trace("{}", foundInSiri.toJavaList());
        for (String routeId : mapRouteIdToLinesOfSiriLog.keySet().toList().sorted()) {
            if (foundInSiri.contains(routeId)) {
//                jsonClient.saveSiriResultsOfRoute(date, routeId, convertToJava(mapRouteAndTrip.get(routeId).get()));
            }
            else {
                // also insert to redis, otherwise next time will again search for them in Siri logs
                logger.info("inserting empty map to Redis for route {}, because Siri logs have no data for it", routeId);
                Map<String, Stream<String>> emptyMap = HashMap.empty();
            }
        }

        // now need to merge these results with mapRouteAndTrip1 (those already in Redis)
        final Map<String, Map<String, Stream<String>>> empty = HashMap.empty();
        final Map<String, Map<String, Stream<String>>> mapAll = empty.merge(mapRouteAndTrip1).merge(mapRouteAndTrip);
        return mapAll;
    }

    private Map<String, Stream<String>> convert(java.util.Map<String,java.util.List<String>> siriResultsOfRoute) {
        Map<String, Stream<String>> result = HashMap.empty();
        for (String key : siriResultsOfRoute.keySet()) {
            result = result.put(key, Stream.ofAll(siriResultsOfRoute.get(key)));
        }
        return result;
    }


    private java.util.Map<String, java.util.List<String>> convertToJava(Map<String, Stream<String>> map) {
        java.util.HashMap<String, java.util.List<String>> mapTrips = new java.util.HashMap<>();
        map.keySet().forEach(key -> {
            mapTrips.put(key, map.get(key).map(stream -> stream.toJavaList()).getOrElse(new ArrayList<>()));
        });
        return mapTrips;
    }



    private Stream<String> readSiriLinesFromFile(final String routeId, final String date) {
        // names: list of names of all siri_rt_data files from the specified date
        // (assumes we won't have more than 20 files of siri results in the same date)
        String fileName = "siri_rt_data_v2." + date + "." + 0 + ".log.gz";
        String fullPath = Utils.findFile(siriLogFilesDirectory, fileName);
        if (fullPath == null) {
            logger.warn("could not find file {} in path {}", fileName, siriLogFilesDirectory);
        }
        else {
            logger.info("found file {} in path {}, full path is {}", fileName, siriLogFilesDirectory, fullPath);
        }
        List<String> namesOldFormat = List.range(0, 20).map(i -> Utils.findFile(siriLogFilesDirectory, "siri_rt_data." + date + "." + i + ".log.gz")).filter(s -> s != null);  // 2019-04-04
        List<String> names = List.range(0, 20).map(i -> Utils.findFile(siriLogFilesDirectory, "siri_rt_data_v2." + date + "." + i + ".log.gz")).filter(s -> s != null);  // 2019-04-04
        names = names.appendAll(namesOldFormat);
        logger.debug("the files are: {}", names.toString());
        logger.info("reading {} siri results log files...", names.size());

        // lines/vLines: Stream/List of all lines from the siri_rt-data file(s) [of day {date}, that belong to route ROUTE_ID
        Stream<String> linesAll = this
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1);
        List<String> allLinesAsList = linesAll.toList();

        Stream<String> lines = allLinesAsList.toStream()
                .filter(line -> gpsExists(line))
                .filter(line -> routeId.equals(this.extractRouteId(line)));

        return lines;
    }

    private Stream<String> fileOrDatabase(String routeId, String date) {
        if (existInDatabase(date, routeId)) {
            logger.debug("retrieving siri rows of route {} and date {} from DB", routeId, date);
//            return result;

            // retrieve the data faster?
            // Map<String, io.vavr.collection.Stream<String>> trips
            StopWatch sw = Utils.stopwatchStart();
            java.util.List<String> list =  getBy(date, routeId);
            sw.stop();
            logger.debug("pure retrieved Stream<String> from DB: {} ms", Utils.stopwatchStopInMillis(sw));
            return Stream.ofAll(list);
        }
        if (existInDatabase(date)) {
            // meaning: siri logs of {date} were read into DB,
            // but this route is not found in the logs, probably because it was not operating
            // on that day
            logger.debug("date {} is in DB, but route {} is not found on that date", date, routeId);
            return Stream.empty();
        }
        else {
            logger.info("Nothing in DB for date {}, retrieving siri rows of route {} and date {} from file...", date, routeId, date);
            // note that this reading does NOT insert to DB so next time will also read from file!!!

            Stream<String> result = readSiriLinesFromFile(routeId, date);

            return result;

        }
    }



    @Autowired
    InsideDataRepository insideDataRepository;

    private boolean existInDatabase(String date) {
        InsideData result = insideDataRepository.findFirstByDate(date);
        return (result != null);
    }

    private boolean existInDatabase(String date, String routeId) {
        InsideData result = insideDataRepository.findFirstByDateAndRouteId(date, routeId);
        return (result != null);
    }

    private java.util.List<String> getBy(String date, String routeId) {
        return insideDataRepository
                .findByDateAndRouteId(date, routeId)
                .stream()
                .map(insideData -> insideData.toString())
                .collect(Collectors.toList());
    }

    /**
     * Process siri_rt_data files of the specified date, to create a data structure of
     * all trips of the specified route, according to data we received from Siri.
     * @param routeId
     * @param date
     * @return
     */
    public Map<String, io.vavr.collection.Stream<String>> findAllTrips(final String routeId, final String date) {
        // actual reading from file happens here:
        StopWatch sw = Utils.stopwatchStart();
        io.vavr.collection.Stream <String> vLines = fileOrDatabase(routeId, date);
        logger.info("retrieved Stream<String> from file/DB: {} ms", Utils.stopwatchStopInMillis(sw) );
        if (vLines.isEmpty()) {
            return HashMap.empty();
        }

        logger.info("grouping by tripId...");

        // map of all trips of {routeId} at day {date}
        // key: tripId
        // value: Stream/List of all lines from the siri_rt-data file(s), that belong to this trip
        sw = Utils.stopwatchStart();
        Map<String, io.vavr.collection.Stream<String>> trips = vLines.groupBy(line -> this.extractTripId(line));
        logger.info("grouped Stream<String> by extracted tripId: {} ms", Utils.stopwatchStopInMillis(sw));
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
        logger.info("{} {} route {}:got {} trips, {} of them suspicious", dayOfWeek, date, routeId, trips.size(), trips.count(trip->trip._2.count(i->true)<28));

        return trips;
    }


    private boolean gpsExists(String line) {
        // in files of old format (v1) the 0,0 is at the end of the line
        if (line.endsWith(",0,0")) {
            return false;
        }
        // 2019-06-10T21:17:04.622,[line 1 v 9369853 oad 2019-06-10 ea 21:52],14,496,1,11654198,2019-06-10T21:30:00,9369853,2019-06-10T21:52:00,2019-06-10T18:58:08,0,0,2019-06-10,50599,2,v2
        String[] spl = line.split(",0,0,") ;
        if (spl.length == 1) return true;
        // spl[1] is the part after ",0,0," - it is supposed to be in the format of 2019-06-10,50599,2,v2
        if (spl[1].endsWith("v2") && spl[1].split(",").length == 4) {
            return false;
        }
        else {

            // maybe line contains ",0,0," but in other fields
            return true;
        }
    }

    /**
     * from GTFS files,
     * build a representation of shape and stops for each trip of that route (in that date)
     * (Siri results are not calculated - saves time)
     * @param date      exact date of the day that is processed. String, format 2019-05-25
     * @param routeId   routeId of the route that is processed
     * @return  a list of TripData objects

    In this version we do not have Siri data. So each TripData will have these data items:
    String routeId;
    String shortName;
    String agencyCode;
    String agencyName;
    String dayOfWeek;
    String date;
    String originalAimedDeparture;
    String gtfsETA;
    String gtfsTripId;
    public Map<Integer, StopsTimeData> stopsTimeData;
    String siriTripId;  // as taken from GTFS TripIdToDate
    String alternateTripId = "";    // not sure?
    StopFeatureCollection stops;    // not sure


     * @throws JsonProcessingException
     */
    public List<TripData> buildFullTripsDataWithoutSiri(String date, String routeId) {
        List<TripData> tripsData = buildTripData(date, routeId);
        if ((tripsData == null) || tripsData.isEmpty()) {
            logger.warn("WARNING: empty or null trips data! (date={}, routeId={}", date, routeId);
            return List.empty();
        }
        //displaySuspiciousTrips(tripsData);
        // now add data about stops (from GTFS)
        logger.info("processing stops of route {} ...", routeId);
        tripsData = enrichTripsWithDataFromGtfs(tripsData, date);
        logger.info("... completed! processing stops of route {}.", routeId);
        return tripsData;
    }

    // combine 2 independant invocations (siri, gtfs)
    public List<TripData> buildFullTripsData2(Map<String,io.vavr.collection.Stream<String>> trips, String date, String routeId) {

        // from GTFS:

        // Result might already be in DB, so first we check there
        // and convert from json back to List<TripData>
        List<TripData> tripsAccordingToGtfsTripIdToDate = List.empty();
        String fromDB = db.readKey(generateKey("gtfs", routeId, date));
        if ((fromDB != null) && !"[]".equals(fromDB)) {
            tripsAccordingToGtfsTripIdToDate = convertFromJson(fromDB);
        }
        else {
            // result not in DB, so calculate it
            tripsAccordingToGtfsTripIdToDate = buildFullTripsDataWithoutSiri(date, routeId);
            // and insert to DB
            db.writeKeyValue(generateKey("gtfs", routeId, date), convertToJson(tripsAccordingToGtfsTripIdToDate));
        }

        // from Siri:
        List<TripData> tripsAccordingToSiri = buildSiriData(trips, date, routeId);

        logger.debug("before combine");
        for (int index = 0 ; index < tripsAccordingToGtfsTripIdToDate.size(); index++) {
            TripData tripData = tripsAccordingToGtfsTripIdToDate.get(index);
            logger.debug("tripData siriTripId={},  alternateTripId={}, gtfsTripId={}, oad={}, stops={}", tripData.siriTripId, tripData.alternateTripId, tripData.gtfsTripId, tripData.originalAimedDeparture, tripData.stops );
            if ((tripData.stops != null) && tripData.stops.features != null) {
                logger.debug("{} stops exist", tripData.stops.features.length);
            }
        }

        // combine
        List<TripData> tripsData = completeWithGtfsData(tripsAccordingToSiri, tripsAccordingToGtfsTripIdToDate);
        logger.debug("after combine");

        if ((tripsData == null) || tripsData.isEmpty()) {
            logger.warn("WARNING: empty or null trips data! (routeId={}, date={})", routeId, date);
            return List.empty();
        }
        else {
            for (int index = 0 ; index < tripsData.size(); index++) {
                TripData tripData = tripsData.get(index);
                logger.debug("tripData siriTripId={},  alternateTripId={}, gtfsTripId={}, oad={}, stops={}", tripData.siriTripId, tripData.alternateTripId, tripData.gtfsTripId, tripData.originalAimedDeparture, tripData.stops );
                if ((tripData.stops != null) && tripData.stops.features != null) {
                    logger.debug("{} stops exist", tripData.stops.features.length);
                }
            }
        }
        displaySuspiciousTrips(tripsData);

        return tripsData;
    }

    /**
     * build a representation of Siri results from the data in the method arguments
     * @param trips - map of all trips of {routeId} at day {date}
     *                  key: tripId
     *                  value: Stream/List of all lines from the siri_rt-data file(s), that belong to this trip
     *
     * @param date      exact date of the day that is processed. String, format 2019-05-25
     * @param routeId   routeId of the route that is processed
     * @return  a list of TripData objects
     * @throws JsonProcessingException
     */
    public List<TripData> buildFullTripsData_not_needed_use_2(Map<String,io.vavr.collection.Stream<String>> trips, String date, String routeId) {

        // only from Siri:
        List<TripData> tripsData = buildTripData_orig_(trips, date, routeId);
        if ((tripsData == null) || tripsData.isEmpty()) {
            logger.warn("WARNING: empty or null trips data! (routeId={}, date={})", routeId, date);
            return List.empty();
        }
        displaySuspiciousTrips(tripsData);

        // without the GTFS reading, we have a nice JSON, we only miss trips that were planned but not executed at all!
        // for example 420 2019-03-31 8:20
        // To know times of all planned trips, we can use the schedule files - in this case "siri.schedule.16.Sunday.json.2019-03-31"
        // (or we can read GTFS files and calculate again. Which is a small addition to
        // reading the time consuming Stops data and times that we can do now if searchGTFS is true)

        if (displayStops) {
            // add GTFS data
            logger.info("processing stops of route {} ...", routeId);
            tripsData = enrichTripsWithDataFromGtfs(tripsData, date);
            logger.info("... completed! processing stops of route {}.", routeId);
        }

        // add data from schedule file
        //tripsData = enrichTripsWithDataFromSchedules(tripsData, date);
        /*
        java.util.List<Tuple2<String, String>> scheduledTrips = findScheduledTrips(date, routeId);

        java.util.List<String> scheduledTripIds = scheduledTrips.stream().map(tup -> tup._1).collect(Collectors.toList());
        java.util.List<String> tripIds = tripsData.stream().map(td -> td.siriTripId).collect(Collectors.toList());
        
        if (scheduledTripIds.containsAll(tripIds)) {
            // very nice - same tripIds
            
            // we want those that are in scheduledTrips but are not in (siri) tripIds
            List<String> tripsNotExecuted = differenceBetweenLists(scheduledTripIds, tripIds);
        }
        */
        return tripsData;
    }

    private TripData mergeTripData(TripData left, TripData right) {
//        // expect:
//        if (! (
//                left.siriTripId.equals(right.siriTripId) &&
//                left.originalAimedDeparture.equals((right.originalAimedDeparture))
//        )) {
//            logger.warn("something went wrong!!");
//        }
//        left.alternateTripId = right.alternateTripId;
        return left;
    }

    private java.util.List<TripData> enrichTripsWithDataFromSchedules(java.util.List<TripData> tripsData, String date) {
        // temporary
        return tripsData;
    }

    private List<String> differenceBetweenLists(java.util.List<String> scheduledTripIds, java.util.List<String> tripIds) {
        return List.empty();
    }

    /**
     * read schedule files
     * @param date
     * @param routeId
     * @return  list of all expected trips (according to GTFS) of the specified route, at the specified date.
     * each item in the list is a Tuple of tripId and AimedDepartureTime (hh:mm)
     */
    private List<Tuple2<String, String>> findScheduledTrips(String date, String routeId) {
        List<Tuple2<String, String>> list = List.empty();
        list.append(Tuple.of("a", "b"));
        return list;
    }


    /**
     *
     * @param tripsData
     * @return

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
    public String convertToJson(final List<TripData> tripsData) {
        logger.info("converting to JSON...");
        try {
            java.util.List javaList = tripsData.sortBy(tripData -> tripData.originalAimedDeparture).toJavaList();
            ObjectMapper x = new ObjectMapper();
            // vavr List does not create good JSON. so use java.util.List
            //String json = x.writeValueAsString(javaList);
            String json = x.writerWithDefaultPrettyPrinter().writeValueAsString(javaList);
            logger.trace("{}",x.writerWithDefaultPrettyPrinter().writeValueAsString(javaList));
            logger.info("                  ... Done");
            return json;
        } catch (JsonProcessingException e) {
            logger.error("exception during marshalling", e);
            return "[]";
        }
    }
    public List<TripData> convertFromJson(String json) {
        if (StringUtils.isEmpty(json) || "[]".equals(json)) {
            return List.empty();
        }
        java.util.List<java.util.HashMap<String, Object>> javaList = new ArrayList<>();
        ObjectMapper x = new ObjectMapper();
        try {
            javaList = x.readValue(json, javaList.getClass());
            // TODO - this converts json to maps, not to Objects
            return List.ofAll(javaList).map(hashMap -> convertToTripData(hashMap));
        } catch (JsonProcessingException e) {
            logger.error("exception when converting json to a list of TripData");
            return List.empty();
        }

        //return List.empty();//ofAll(javaList);
    }

    private TripData convertToTripData(java.util.HashMap<String, Object> hashMap) {
        TripData td = new TripData();
        td.alternateTripId = hashMap.get("alternateTripId").toString();
        td.originalAimedDeparture = hashMap.get("originalAimedDeparture").toString();
        td.dayOfWeek = hashMap.get("dayOfWeek").toString();
        td.siriTripId = hashMap.get("siriTripId").toString();
        td.routeId = hashMap.get("routeId").toString();
        td.date = hashMap.get("date").toString();
        td.dns = ((hashMap.get("dns") == null)?false:Boolean.parseBoolean(hashMap.get("dns").toString()));
        td.suspicious = ((hashMap.get("suspicious") == null)?false:Boolean.parseBoolean(hashMap.get("suspicious").toString()));
        td.agencyCode = (null==hashMap.getOrDefault("agencyCode", ""))?"":hashMap.getOrDefault("agencyCode", "").toString();
        td.agencyName = (null==hashMap.getOrDefault("agencyName", ""))?"":hashMap.getOrDefault("agencyName", "").toString();
        td.shortName = (null==hashMap.getOrDefault("shortName", ""))?"":hashMap.getOrDefault("shortName", "").toString();
        td.vehicleId = (null==hashMap.getOrDefault("vehicleId", ""))?"":hashMap.getOrDefault("vehicleId", "").toString();
        td.gtfsTripId = (null==hashMap.getOrDefault("gtfsTripId", ""))?"":hashMap.getOrDefault("gtfsTripId", "").toString();
        td.gtfsETA = (null==hashMap.getOrDefault("gtfsETA", ""))?"":hashMap.getOrDefault("gtfsETA", "").toString();

        td.siri = convertToSiri (hashMap.get("siri"));
        //td.stopsTimeData;
        td.stops = convertToStops(hashMap.get("stops"));

        return td;
    }

    private StopFeatureCollection convertToStops(Object stops) {
        if (stops == null) return null;
        java.util.HashMap<String, Object> siriMap = new java.util.HashMap<>();
        StopFeature[] features = convertStopFeatureList((java.util.Map<String, Object>) stops);
        StopFeatureCollection stopFeatureCollection = new StopFeatureCollection(features);
        return stopFeatureCollection;
    }

    private StopFeature[] convertStopFeatureList(java.util.Map<String, Object> stops) {
        java.util.List<Object> list = (java.util.List<Object>) stops.get("features");
        java.util.List<StopFeature> sfs = new ArrayList<>();
        for (Object obj : list) {
            java.util.Map<String, Object> map = (java.util.Map<String, Object>) obj;
            //Object geometryMap = map.get("geometry")
            //Object propertiesMap =  map.get("properties")
            sfs.add(convertToFeature(map));
        }
        return sfs.toArray(new StopFeature[]{});
    }

    private StopFeature convertToFeature(java.util.Map<String, Object> map) {
        StopFeature sf = new StopFeature();
        sf.type = (String) map.get("type");
        sf.geometry = convertToGeometry (map.get("geometry"));
        sf.properties = convertToProperties(map.get("properties"));
        return sf;
    }

    private StopProperties convertToProperties(Object properties) {
        java.util.Map<String, String> map = (java.util.Map<String, String>) properties;
        StopProperties sp = new StopProperties();
        sp.stop_sequence = map.get("stop_sequence");
        sp.departureTime = map.get("departureTime");
        sp.arrivalTime = map.get("arrivalTime");
        sp.zone_id = map.get("zone_id");
        sp.location_type = map.get("location_type");
        sp.distance = map.get("distance");
        sp.stop_desc = map.get("stop_desc");
        sp.stop_name = map.get("stop_name");
        sp.stop_code = map.get("stop_code");
        sp.stop_id = map.get("stop_id");
        sp.trip_id = map.get("trip_id");
        sp.parent_station = map.get("parent_station");
        return sp;
    }

    private PointGeometry convertToGeometry(Object geometry) {
        PointGeometry pg = new PointGeometry();
        pg.type = ((java.util.Map<String, String>)geometry).get("type");
        pg.coordinates = convertToCoordinates(((java.util.Map<String, Object>)geometry).get("coordinates"));
        return pg;
    }

    private String[] convertToCoordinates(Object coordinates) {
        java.util.List<String> coords = (java.util.List<String>) coordinates;
        return new String[] {coords.get(0), coords.get(1)};
    }

    private SiriFeatureCollection convertToSiri(Object siri) {
        if (siri == null) return null;
        java.util.LinkedHashMap<String, Object> siriMap = (java.util.LinkedHashMap) siri;
        SiriFeature[] features = convertFeatureList(siriMap);
        SiriFeatureCollection siriFeatureCollection = new SiriFeatureCollection(features);
        return siriFeatureCollection;
    }

    private SiriFeature[] convertFeatureList(java.util.LinkedHashMap<String, Object> siri) {
        //LinkedHashMap map1 = (LinkedHashMap) siri;
        java.util.List<Object> listOfObjects = (java.util.List<Object>) siri.get("features");
        // convert each object to a SiriFeature
        List<SiriFeature> sfs = List.empty();
        for (Object obj : listOfObjects) {
            java.util.LinkedHashMap<String, Object> map2 = (java.util.LinkedHashMap<String, Object>) obj ;
            SiriFeature sf = new SiriFeature();
            sf.geometry = convertGeometry(map2.get("geometry"));
            sf.properties = convertProperties(map2.get("properties"));
            sfs = sfs.append(sf);
        }
        SiriFeature[] result = sfs.toJavaList().toArray(new SiriFeature[]{});
        return result;

    }

    private SiriProperties convertProperties(Object properties) {
        java.util.LinkedHashMap<String, String> map = (java.util.LinkedHashMap<String, String>) properties;
        SiriProperties sp = new SiriProperties();
        sp.time_recorded = map.get("time_recorded");
        sp.recalculatedETA = map.get("recalculatedETA");
        sp.timestamp = map.get("timestamp");
        if (map.containsKey("distanceFromStart")) {
            sp.distanceFromStart = Long.parseLong( map.get("distanceFromStart") );
        }
        return sp;
    }

    private PointGeometry convertGeometry(Object geometry) {
        java.util.LinkedHashMap<String, Object> map = (java.util.LinkedHashMap<String, Object>) geometry;
        Object obj = map.get("coordinates");
        java.util.List<String> coords = (ArrayList<String>) obj ;
        PointGeometry pg = new PointGeometry();
        pg.coordinates = coords.toArray(new String[]{});
        return pg;
    }

    private static String getTripId(TripData tripData) {
        if (!StringUtils.isEmpty(tripData.siriTripId)) return tripData.siriTripId;
        else if (!StringUtils.isEmpty(tripData.gtfsTripId)) return tripData.gtfsTripId;
        else if (!StringUtils.isEmpty(tripData.alternateTripId)) return tripData.alternateTripId;
        return "";
    }

    private static String getGtfsTripId(TripData tripData) {
        if (!StringUtils.isEmpty(tripData.gtfsTripId)) return tripData.gtfsTripId;
        else if (!StringUtils.isEmpty(tripData.alternateTripId)) return tripData.alternateTripId;
        else if (!StringUtils.isEmpty(tripData.siriTripId)) return tripData.siriTripId;
        return "";
    }


    public TripData enrichSingleTrip(TripData tripData, Map<String, Map<Integer, StopsTimeData>> allStopsOfAllTrips) {
        if (allStopsOfAllTrips.isEmpty()) return tripData;
        ///////////////////////////////////////////////
        //
        // use the alternate (gtfs) tripId here, because allStopsOfAllTrips was generated from GTFS, so all keys are compound trip ids
        String theTripId = getGtfsTripId(tripData);
        //
        ///////////////////////////////////////////////
        logger.debug("enrich single trip {}", theTripId);
        Map<Integer, StopsTimeData> stopsTimeData = allStopsOfAllTrips.getOrElse(theTripId, null);
        if (stopsTimeData == null) {
            logger.debug("data about stops of trip {} does not exist yet, searching this trip again According To Departure Hour...", theTripId);
            String matchingTripAccordingToDepartureHour = null;
            for (String altTripId : allStopsOfAllTrips.keySet()) {
                String depTime = "";
                try {
                    // it could be tripData.originalAimedDeparture = "11:45"
                    // and allStopsOfAllTrips.get(altTripId).get(1).departureTime = "11:45:00"
                    // so we don't check with equals() - we check the time difference between them
                    //if (tripData.originalAimedDeparture.equals(allStopsOfAllTrips.get(altTripId).get(1).departureTime)) {
                    LocalTime lt = null;
                    if (tripData.originalAimedDeparture.contains("T")) {
                        lt = LocalDateTime.parse(tripData.originalAimedDeparture).toLocalTime();
                        // TODO handle the case of same time but of the day before (2019-10-15T23:45:00)
                    }
                    else {
                        lt = LocalTime.parse(tripData.originalAimedDeparture);
                    }
                    depTime = allStopsOfAllTrips.get(altTripId).get().get(1).get().departureTime;
                    if (depTime == null) {
                        logger.debug("departureTime for alternateTripId {} is null", altTripId);
                    }
                    else if (lt.compareTo(
                            LocalTime.parse(depTime)) == 0) {
                        matchingTripAccordingToDepartureHour = altTripId;
                        break;
                    }
                }
                catch (Exception ex) {
                    // tripData.originalAimedDeparture is sometimes 24:00 or even 25:00
                    // this causes DateTimeParseException
                    logger.warn("absorbing, departureTime={}", depTime, ex);
                }
            }
            if (matchingTripAccordingToDepartureHour != null) {
                tripData.alternateTripId = matchingTripAccordingToDepartureHour;
                logger.debug("found matching trip with the same hour {}, generating stops of trip {}", tripData.originalAimedDeparture, tripData.alternateTripId);
                Map<Integer, StopsTimeData> temporaryStopsTimeData = allStopsOfAllTrips.get(tripData.alternateTripId).get();
                logger.debug("creating featureCollection with all stops of trip {}", tripData.alternateTripId);
                StopFeatureCollection sfc = StopsTimeData.createFeatures(temporaryStopsTimeData);
                sfc.features = List.of(sfc.features).sortBy(stopFeature -> stopFeature.properties.stop_sequence).toJavaArray(StopFeature.class);
                tripData.stops = sfc;
            }
        }
        else {
            // no need to put in td both stopsTimeData and stops - they contain exactly the same information
            // removing it will make the JSON much smaller!
            // - not needed: tripData.stopsTimeData = allStopsOfAllTrips.get(tripData.siriTripId);
            logger.debug("retrieving stops of existing trip {}", theTripId);
            Map<Integer, StopsTimeData> temporaryStopsTimeData = allStopsOfAllTrips.get(theTripId).get();
            logger.debug("creating featureCollection with all stops of existing trip {}", tripData.siriTripId);
            StopFeatureCollection sfc = StopsTimeData.createFeatures(temporaryStopsTimeData);
            // sort the features according to stop_sequence
            sfc.features = List.of(sfc.features).sortBy(stopFeature -> Integer.parseInt(stopFeature.properties.stop_sequence)).toJavaArray(StopFeature.class);
            tripData.stops = sfc;
        }
        return tripData;    // returning a tripData with 1 or 2 changes - populated the "stops" with features,
                            // and possibly also populated alternateTripId
    }

    public List<TripData> enrichTripsWithDataFromGtfs(final List<TripData> tripsData, final String date) {
        if (logger.isDebugEnabled()) {
            logger.debug("enrichTripsWithDataFromGtfs: in: date={}, trips=", date);
            for (TripData tripData : tripsData) {
                logger.debug("{}", tripData);
            }
        }
        // tripsData is trips that we found in Siri. But sometimes these trip IDs are not found in GTFS???
        if (searchGTFS) {   // this part searches in GTFS files, which is very time consuming!
            // GTFS data is needed for displaying the stops in Leaflet widget UI
            // also - SIRI data might not contain some trips, if They were not actually executed! So we need to get the list of planned trips from GTFS
            // also - the Trip IDs that SIRI reports might not exist at all in GTFS(!) - this is probably because of a bug in GTFS
            // (They use same tripIDs for all week days, but it should be different trip IDs each day. The TripIdToDate file contains the correct data)
            // it is possible to get the list of planned trips from the schedule files of that day (if you have them)
            logger.info("reading data about stops, from GTFS file ...");
            Set<String> tripIds = findAllTripIds(tripsData);
            logger.info("trip ids: {}", io.vavr.collection.List.ofAll(tripIds).sorted().toJavaList());

            String gtfsZipFileName = decideGtfsFileName(date);
            final String gtfsZipFileFullPath = Utils.findFile(directoryOfGtfsFile, gtfsZipFileName);
            final List<String> tripLines = List.ofAll( tripFileReader.retrieveTripLinesFromFile(gtfsZipFileFullPath) );
            // actually these should be named gtfsTripIds
            List<String> alternateIds = findAlternateTripIds(tripsData.get(0).routeId, date, tripLines, CalendarReader.make(directoryOfGtfsFile).readCalendar(date));
            logger.info("alternate trip ids: {}", io.vavr.collection.List.ofAll(alternateIds).sorted().toJavaList());
            // add the alternateID to tripData.gtfsAlternateId
            tripsData.forEach(td -> td.alternateTripId = alternateIds.find(id -> id.startsWith(td.siriTripId + "_")).getOrElse(""));

            logger.debug("retrieve from GTFS data about stops");
            final Map<String, Map<Integer, StopsTimeData>> allFinal = stops.generateStopsMap1(alternateIds.toSet(), date, false);
            logger.info("stops map from alternate trip Ids: {}",allFinal.toString());

/*
            logger.debug("retrieve from GTFS data about stops");
            Map<String, Map<Integer, StopsTimeData>> all = stops.generateStopsMap1(tripIds, date, true);  // why empty?????
            logger.debug("retrieve from GTFS:  map of tripId to StopsData = {}, keys={}", all, all.keySet());   // here always empty map? because tripIds are without _{dateBegin}
            //Map<Integer, StopsTimeData> res = findAlternateTripId(tripsData.get(0).routeId, "", date);
            if (all.isEmpty() || all.size() < tripIds.size()) {
                if (!alternateIds.isEmpty()) {
                    logger.warn("trip ids not found in stops map of GTFS. Trying to find by aimedDepartureTime...");
                    //List<String> alternateIds = findAlternateTripIds(tripsData.get(0).routeId, date);
                    logger.info("alternate tripIds: {}", alternateIds.toJavaList());
                    Map<String, Map<Integer, StopsTimeData>> all2 =
                            stops.generateStopsMap1(alternateIds.toSet(), date, true);
                    logger.info("stops map from alternate trip Ids: {}",all2.toString());
                    all = all.merge(all2);
                    logger.info("merged stops map: {}", all.toString());
                    //Set<Tuple2<String, String>> tripIdsWithAimedDepartureTimes = findAllTripIdsWithAimedDepartureTimes(tripsData);
                    // each tuple is (tripId, aimedDepartureTime) - maybe should be routeId and aimedDepartureTime?
                    // BUG???  I think it should be here:
                    //all = stops.generateStopsMap(tripIdsWithAimedDepartureTimes, date);
                    // in any case, currently generateStopsMap() this does not do anything!
                    //stops.generateStopsMap(tripIdsWithAimedDepartureTimes, date);
                }
                else {
                    logger.warn("no alternate IDs found!!! This is usually because trip IDs in Siri logs do not match trip IDs in GTFS (trips.txt)");
                }
            }

            // returning an updated TripsData (with each of its tripData items updated to include "stops"
            final Map<String, Map<Integer, StopsTimeData>> allFinal = all;
*/
            if (allFinal.isEmpty()) {
                logger.debug("no data retrieved from GTFS about stops!");
            }
            List<TripData> result = tripsData.map(tripData -> enrichSingleTrip(tripData, allFinal));
            if (logger.isDebugEnabled()) {
                logger.debug("enrichTripsWithDataFromGtfs: out: trips=");
                for (TripData tripData : tripsData) {
                    logger.debug("{}", tripData);
                }
            }
            return result;
//            tripsData.forEach(tripData -> tripData.stopsTimeData = all.get(tripData.siriTripId));
//            logger.info("                  ... stopsTimeData Done");
//            tripsData.forEach(tripData -> {
//                tripData.stops = StopsTimeData.createFeatures(tripData.stopsTimeData);
//            });
//            logger.info("                  ... stopsFeatureCollection Done");
        }
        return tripsData;
    }

    private void displaySuspiciousTrips( List<TripData> tripsData) {
        List<String> suspicious = tripsData
                                .filter(trip -> "true".equals( trip.suspicious))
                                .map(tripData -> tripData.siriTripId);
        suspicious.forEach(tripId -> {
            int numberOfSiriPoints = tripsData
                    .filter(tripData -> tripData.siriTripId.equals(tripId))
                    .map(td -> td.siri.features.length).get(0);
            logger.info("trip {} is suspicious: has only {} GPS points", tripId, numberOfSiriPoints);
        });

        List<String> dnsTrips = tripsData
                                .filter(trip -> "true".equals( trip.dns))
                                .map(tripData -> tripData.siriTripId);
        logger.info("DNS trips:{}", dnsTrips);
    }


    private Set<String> findAllTripIds(List<TripData> tripsData) {
        //Set<String> aimedTimesOfAllTrips = tripsData.stream().map(tripData -> tripData.originalAimedDeparture).collect(Collectors.toSet());
        Set<String> tripIds = tripsData.map(tripData -> tripData.siriTripId).toSet();
        return tripIds;
    }

    private Set<Tuple2<String, String>> findAllTripIdsWithAimedDepartureTimes(List<TripData> tripsData) {
        Set<String> aimedTimesOfAllTrips = tripsData.map(tripData -> tripData.originalAimedDeparture).toSet();
        Set<String> tripIds = tripsData.map(tripData -> tripData.siriTripId).toSet();
        io.vavr.collection.Stream<String> vt = io.vavr.collection.Stream.ofAll(tripIds);
        io.vavr.collection.Stream<Tuple2<String, String>> tuples = vt.zip(io.vavr.collection.Stream.ofAll(aimedTimesOfAllTrips));
        Set<Tuple2<String, String>> tuplesAsSet = tuples.toList().toSet();
        return tuplesAsSet;
    }

    // this version uses only TripIdToDate, so it does not need to read siri log files!!
    public List<TripData> buildTripData(String date, String routeId) {

        List<TripData> tripsAccordingToGtfsTripIdToDate = buildTripsFromTripIdToDate(routeId, date);
        List<TripData> sorted = tripsAccordingToGtfsTripIdToDate.sortBy(tripData -> tripData.getOriginalAimedDeparture());
        return sorted;
    }

    public List<TripData> buildSiriData(Map<String, io.vavr.collection.Stream<String>> trips, String date, String routeId) {
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();

        List<TripData> tripsAccordingToSiri =
                trips.keySet().map(tripId ->
                {
                    io.vavr.collection.Stream<String> thisTrip = trips.get(tripId).get();
                    String firstLine = thisTrip.head();
                    TripData td = new TripData();
                    td.routeId = routeId;
                    td.date = date;
                    td.dayOfWeek =  Integer.toString( dayOfWeek.getValue() ) ;
                    //dayOfWeek.toString();
                    td.siriTripId = tripId;
                    //td.siri1 = thisTrip.map(line -> createSiriReading(line)).toJavaList();
                    td.siri = new SiriFeatureCollection(thisTrip.map(line -> createSiriPart(line)).filter(sf -> !zeroCoordinates(sf.geometry.coordinates)).asJava().toArray(new SiriFeature[]{}));
                    td.vehicleId = extractVehicleId(firstLine);
                    td.agencyCode = extractAgency(firstLine);
                    td.agencyName = agencyNameFromCode(td.agencyCode);
                    td.shortName = extractShortName(firstLine);
                    td.originalAimedDeparture = extractAimedDeparture(firstLine);
                    td.suspicious = false;
                    if (td.siri.features.length < 20) {td.suspicious = true;};
                    return td;
                }).toList();
        return tripsAccordingToSiri;
    }

    private boolean zeroCoordinates(String[] coordinates) {
        return ((coordinates.length == 2) &&
                "0".equals(coordinates[0]) &&
                "0".equals(coordinates[1]) );
    }

    /**
     * build POJO representation of trips from the data in the method arguments
     * TODO cache the result of calling this method
     * @param trips - map of all trips of {routeId} at day {date}
     *                  key: tripId
     *                  value: Stream/List of all lines from the siri_rt-data file(s), that belong to this trip
     *
     * @param date
     * @param routeId
     * @return          list of all trips (according to Siri data) of the specified route on the specified date.
     *              Note that Siri data might not contain ALL planned trips. Because possibly some trips were
     *              not executed at all.
     *              To know also planned trips that do not appear in Siri Data, we need to extract this information
     *              from GTFS (or from TripIdToDate, or from schedule files that were created by GTFS-Collector)
     */
    // sample of a line:
    // 2019-04-04T17:25:12.187,[line 358 v 8335053 oad 15:20 ea 18:17],15,8136,358,37350079,15:20,8335053,18:17,17:24:47,31.802471160888672,34.83134460449219
    public List<TripData> buildTripData_orig_(Map<String, io.vavr.collection.Stream<String>> trips, String date, String routeId) {
        // following 2 lines not dependent, can be done in parallel
        List<TripData> tripsAccordingToGtfsTripIdToDate = buildFullTripsDataWithoutSiri(date, routeId);
        List<TripData> tripsAccordingToSiri = buildSiriData(trips, date, routeId);

        //List<TripData> tripsAccordingToGtfsTripIdToDate = buildTripData(date, routeId);

        // not needed?
        //Map<String, String> gtfsTrips = findSiriTrips(List.ofAll(tripsAccordingToGtfsTripIdToDate));
        //display(gtfsTrips);


        List<TripData> sortedTripsAccordingToSiri = completeWithGtfsData(tripsAccordingToSiri, tripsAccordingToGtfsTripIdToDate);

        return sortedTripsAccordingToSiri;
    }

    /**
     * find in gtfsTrips (by originAimedDeparture time) the trips that do not appear in siriTrips
     * and add them to the siri list (with indication that they are DNS in Siri)
     * @param tripsAccordingToSiri
     * @param tripsAccordingToGtfsTripIdToDate
     * @return
     */
    private List<TripData> completeWithGtfsData(List<TripData> tripsAccordingToSiri, List<TripData> tripsAccordingToGtfsTripIdToDate) {
        if ((tripsAccordingToSiri == null) || (tripsAccordingToSiri.isEmpty())) {
            return tripsAccordingToGtfsTripIdToDate;
        }
        String date = tripsAccordingToSiri.head().originalAimedDeparture.split("T")[0];
        java.util.List<String> gtfsHours = tripsAccordingToGtfsTripIdToDate.map(tripData -> tripData.getOriginalAimedDeparture()).collect(Collectors.toList());
        java.util.List<String> siriHours1 = tripsAccordingToSiri.map(tripData -> tripData.getOriginalAimedDeparture()).collect(Collectors.toList());
        // in siri v2, hour will be of the format 2019-07-28T06:45:00, so in order to compare with gtfs hour 06:45 we will strip date part from siri hour
        java.util.List<String> fixedSiriHours = List.ofAll(siriHours1)
                .map(hour -> {
                        if (hour.contains("T")) {
                            String hourPart = hour.split("T")[1];
                            // assuming exactly the format 06:45:00, we take 5 first characters
                            hourPart = hourPart.substring(0,5);
                            return hourPart;
                        }
                        else {
                            return hour;
                        }
                    })
                .asJava();
        List<String> gtfsHoursNotInSiri = List.ofAll(gtfsHours).removeAll(hour -> fixedSiriHours.contains(hour));

        List<TripData> siri = List.empty();  //.ofAll(tripsAccordingToSiri);
        for (String hour : gtfsHoursNotInSiri) {
            TripData gtfsTripWithThatOad = findGtfsTripWithOriginAimeDeparture(tripsAccordingToGtfsTripIdToDate,hour);
            TripData tr = gtfsTripWithThatOad;
            tr.setOriginalAimedDeparture(fixHour(tr.getOriginalAimedDeparture(), date));
            logger.warn("found gtfs trip with OAD {} that did not exist in Siri trips: {}/{}/{}", hour, tr.siriTripId, tr.gtfsTripId, tr.alternateTripId);
            tr.dns = true;
            siri = siri.append(tr);
        }
        // merge gtfs data of all GTFS trips that DO exist in Siri (otherwise, we don't get data about stops!!!)
        for (TripData td : tripsAccordingToSiri) {
            TripData merged = mergeGtfsTripData(td, tripsAccordingToGtfsTripIdToDate);
            siri = siri.append(merged);
        }
        // sort again (by oad) and return
        siri = siri.sortBy(tripData -> tripData.getOriginalAimedDeparture());
        return siri;
    }

    private String fixHour(String originalAimedDeparture, String date) {
        if (!originalAimedDeparture.contains("T")) {
            return date + "T" + originalAimedDeparture;
        }
        else {
            return originalAimedDeparture;
        }
    }

    private TripData mergeGtfsTripData(TripData td, List<TripData> tripsAccordingToGtfsTripIdToDate) {
        TripData gtfsTrip = findGtfsTripWithOriginAimeDeparture(tripsAccordingToGtfsTripIdToDate, td.originalAimedDeparture);
        if (null != gtfsTrip) {
            if (gtfsTrip.stops != null) {
                td.stops = gtfsTrip.stops;
            }
            if (!td.siriTripId.equals(gtfsTrip.siriTripId)) {
                td.gtfsTripId = gtfsTrip.siriTripId;
            }
            if (StringUtils.isEmpty(td.gtfsETA))                    td.gtfsETA = gtfsTrip.gtfsETA;
            if (StringUtils.isEmpty(td.agencyCode))                 td.agencyCode = gtfsTrip.agencyCode;
            if (StringUtils.isEmpty(td.agencyName))                 td.agencyName = gtfsTrip.agencyName;
            if (StringUtils.isEmpty(td.alternateTripId))            td.alternateTripId = gtfsTrip.alternateTripId;
            if (StringUtils.isEmpty(td.date))                       td.date = gtfsTrip.date;
            if (StringUtils.isEmpty(td.dayOfWeek))                  td.dayOfWeek = gtfsTrip.dayOfWeek;
            if (StringUtils.isEmpty(td.originalAimedDeparture))     td.originalAimedDeparture = gtfsTrip.originalAimedDeparture;
            if (StringUtils.isEmpty(td.routeId))                    td.routeId = gtfsTrip.routeId;
            if (StringUtils.isEmpty(td.shortName))                  td.shortName = gtfsTrip.shortName;
        }
        return td ;
    }

    TripData findGtfsTripWithOriginAimeDeparture(List<TripData> trips, String hour) {
        if ((trips == null) || StringUtils.isEmpty(hour)) return null;
        final String fixedHour = fix(hour);
        TripData tripWithThatOad = List.ofAll(trips)
                .find(tripData -> fixedHour.equals(tripData.getOriginalAimedDeparture())).getOrElse(() -> null);
        return tripWithThatOad;
    }

    private String fix(final String hour) {
        String result = hour;
        if (hour.contains("T")) {
            result = hour.split("T")[1];
        }
        if (2 == StringUtils.countOccurrencesOf(result, ":")) {
            int index = result.lastIndexOf(":");
            result = result.substring(0, index);
        }
        return result;
    }

    private void display(Map<String, String> siriTrips) {
        List<Tuple2<String, String>> sorted = siriTrips.toList().sorted();
        //logger.info("all trips: {}",siriTrips.keySet().toSortedSet().map(key -> "trip/" + key + "/" + siriTrips.get(key)).toJavaStream().collect(Collectors.joining(",")));
        logger.info("all trips: {}", siriTrips.toString());
    }


    // returns a Map of key=departureTime and value=tripId
    // tripId is taken from Siri data.
    private Map<String, String> findSiriTrips(List<TripData> allTripsOfDay) {
        Map<String,String> departureToTripId = HashMap.ofEntries(
                    allTripsOfDay
                    .map(tripData -> Tuple.of(tripData.originalAimedDeparture, tripData.siriTripId))
                    .sortBy(tuple -> tuple._2)
        );
        return departureToTripId;
    }


    /**
     *
     * This method is supposed to be a nicer implementation instead of  public java.util.List<TripData> buildTripData(...)
     * But I need to add here the "complete trips from GTFS TripIdToDate" part.
     *
     *
     * Build TripData objects for all trips of that route and that day
     * @param trips     all lines in the siri logs that belong to each trip. can be retrieved by calling findAllTrips(routeId, date)
     * @param date
     * @param routeId
     * @return      list of all trips (according to Siri data) of the specified route on the specified date.
     */
    public List<TripData> buildTripsData(Map<String, io.vavr.collection.Stream<String>> trips, String date, String routeId) {
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
        List<TripData> tripsAccordingToSiri =
                trips
                        .keySet()
                        .map(tripId -> buildTripData2(tripId, trips.get(tripId).getOrElse(io.vavr.collection.Stream.empty()), date, routeId, dayOfWeek))
                        .toList();
        List<TripData> sortedTripsAccordingToSiri = tripsAccordingToSiri.sortBy(tripData -> tripData.originalAimedDeparture);
        return sortedTripsAccordingToSiri;
    }


    /**
     * Build data for a single trip
     * @param tripId
     * @param tripLinesInLog    all lines in the siri logs that belong to that trip. can be retrieved by calling findAllTrips(routeId, date).get(tripId)
     * @param date
     * @param routeId
     * @param dayOfWeek
     * @return
     */
    private TripData buildTripData2(String tripId, io.vavr.collection.Stream<String> tripLinesInLog, String date, String routeId, DayOfWeek dayOfWeek) {
        io.vavr.collection.Stream<String> thisTrip = tripLinesInLog;
        String firstLine = thisTrip.head();
        TripData td = new TripData();
        td.routeId = routeId;
        td.date = date;
        td.dayOfWeek =  Integer.toString( dayOfWeek.getValue() ) ;
        //dayOfWeek.toString();
        td.siriTripId = tripId;
        td.siri = new SiriFeatureCollection(new SiriFeature[]{});
        //td.siri1 = thisTrip.map(line -> createSiriReading(line)).toJavaList();

        // This should be called later, separately
        td.siri = new SiriFeatureCollection(thisTrip.map(line -> createSiriPart(line)).asJava().toArray(new SiriFeature[]{}));


        td.vehicleId = extractVehicleId(firstLine);
        td.agencyCode = extractAgency(firstLine);
        td.agencyName = agencyNameFromCode(td.agencyCode);
        td.shortName = extractShortName(firstLine);
        td.originalAimedDeparture = extractAimedDeparture(firstLine);
        td.suspicious = false;
        if (td.siri.features.length < 20) {td.suspicious = true;};
        return td;
    }

    // sort tripsAccordingToSiri again (by originalAimedDeparture)
    private java.util.List<TripData> sort(java.util.List<TripData> tripsAccordingToSiri) {

        return List.ofAll(tripsAccordingToSiri)
                .sortBy(tripData -> tripData.originalAimedDeparture)
                .toJavaList();
        //return tripsAccordingToSiri;
    }


    private Stream<String> readMakatFile(String date) {
        String makatZipFileName = "TripIdToDate" + date + ".zip";    // TripIdToDate2019-05-17.zip
        String makatZipFileFullPath = Utils.findFile(directoryOfMakatFile, makatZipFileName);
        if (makatZipFileFullPath == null) {
            logger.warn("could not fine file {}, path used was: {}", makatZipFileFullPath, directoryOfMakatFile);
        }
        return (new ReadZipFile()).makatLinesFromFile(makatZipFileFullPath);
    }

    private List<TripData> findMissing(List<TripData> tripsAccordingToSiri, java.util.List<TripData> tripsAccordingToGtfsTripIdToDate) {
        List<TripData> missingInSiri = List.empty();
        for (TripData titdTripData : tripsAccordingToGtfsTripIdToDate) {
            if (!containsByTripId(tripsAccordingToSiri, titdTripData)) {
                titdTripData.dns = true;
                missingInSiri.append(titdTripData);
            }
        }
        return missingInSiri;
    }

    private boolean containsByTripId(List<TripData> tripsAccordingToSiri, TripData titdTripData) {
        return tripsAccordingToSiri.toJavaStream().anyMatch(tripData -> tripData.siriTripId.equals(titdTripData.siriTripId));
    }

    /**
     * use TripIdToDate (from GTFS) to build trips for each route from the specified list.
     * This method saves time by reading TripIdToDate only once
     * @param routeIds
     * @param date
     * @return      a map, key=routeId, value=list of trips that this route has on the specified date
     */
    public Map<String, List<TripData>> buildTripsForAllRoutesFromTripIdToDate(List<String> routeIds, String date) {
        logger.info("start building all trips for {} routes {}", routeIds.size(), routeIds);
        // allTrips is a big list but still it is better to have all of it in memory
        // because we are going to use it for all routes now
        List<String> allTrips = readMakatFile(date).toList();
        Map<String, List<TripData>> tripsForAllRoutes = HashMap.empty();
        for (String routeId : routeIds) {
            // for each route, we build trips with the "allTrips" object (that was computed once)
            List<TripData> tripsData = buildTripsFromTripIdToDate(routeId, date, allTrips);
            tripsForAllRoutes = tripsForAllRoutes.put(routeId, tripsData);
        }
        logger.info("completed building all trips for {} routes, map contains trips for {} routes", routeIds.size(), tripsForAllRoutes.keySet().size());
        return tripsForAllRoutes;
    }

    public List<TripData> buildTripsFromTripIdToDate(String routeId, String date, List<String> allTrips) {
        logger.debug("looking for TripIdToDate trips of route {} on {}", routeId, date);
        try {
            java.util.List<TripData> result = allTrips
                            .filter(line -> line.startsWith(routeId + ","))
                            // now filter by dayInWeek
                            .filter(line -> dayInWeekIs(line, calcDayInWeek(date)))
                            // previous filter is not enough, because some lines are for different date range
                            .filter(line -> matchDateRange(line, date))
                            .map(line -> {
                                logger.trace(line);
                                return line;
                            })
                            .map(line -> createTripData(line, date))
                            .collect(Collectors.toList());
            // sort by td.originalAimedDeparture
            List<TripData> sortedResult = List.ofAll(result).sortBy(td -> td.originalAimedDeparture);
            // note that in this file we have only hh:mm - no date
            // might be a problem if some trips are after midnight
            //result = enrichAlternateTripId(result);
            logger.debug("looking for TripIdToDate trips         ... Completed ({} trips)", result.size());
            return sortedResult;
        }
        catch (Exception ex) {
            logger.error("exception while reading from TripIdToDate. continue without that data", ex);
            return List.empty();
        }
    }

    public List<TripData> buildTripsFromTripIdToDate(String routeId, String date) {
        logger.info("looking for TripIdToDate trips of route {} on {}", routeId, date);
        try {
            Stream<String> allTrips = readMakatFile(date);

            List<TripData> result =
                    allTrips
                    .filter(line -> line.startsWith(routeId + ","))
                    // now filter by dayInWeek
                    .filter(line -> dayInWeekIs(line, calcDayInWeek(date)))
                            // TODO that filter is not enough, because some lines are for different date range
                    .filter(line -> matchDateRange(line, date))
                    .map(line -> {
                                logger.info(line);
                                return line;
                            })
                    .map(line -> createTripData(line, date))
                    .toList();
            // sort by td.originalAimedDeparture
            List<TripData> sortedResult = List.ofAll(result).sortBy(td -> td.originalAimedDeparture);
            // note that in this file we have only hh:mm - no date
            // might be a problem if some trips are after midnight
            //result = enrichAlternateTripId(result);
            logger.info("                                       ... Completed ({} trips)", result.size());
            return sortedResult;
        }
        catch (Exception ex) {
            logger.error("exception while reading from TripIdToDate. continue without that data", ex);
            return List.empty();
        }
    }

    private boolean matchDateRange(String line, String date) {
        String dateFrom = extractDateFrom(line);
        String dateTo = extractDateTo(line);
        dateFrom = dateFrom.split(" ")[0];
        dateTo = dateTo.split(" ")[0];
        Integer[] localDateTo = extractDate(dateTo, "/");
        Integer[] localDateFrom = extractDate(dateFrom, "/");
        Integer[] myDate = extractDate(date, "-");
        LocalDate theDate = LocalDate.of(myDate[0], myDate[1], myDate[2]);
        LocalDate date2 = LocalDate.of(localDateTo[2], localDateTo[1], localDateTo[0]);     // reverse because in lines of TripIdToDate the date appears as 11/06/2019
        LocalDate date1 = LocalDate.of(localDateFrom[2], localDateFrom[1], localDateFrom[0]);
        boolean result = theDate.isEqual(date1) || theDate.isEqual(date2)
                ||  (theDate.isAfter(date1) && theDate.isBefore(date2)) ;
        logger.trace("{}: {}  {}  {}", result, date1, date, date2 );
        return result;
    }

    private Integer[] extractDate(String dateTo, String splitOn) {
        String[] strs = dateTo.split(splitOn);
        int year = Integer.parseInt( strs[0] );
        int month = Integer.parseInt( strs[1] );
        int day = Integer.parseInt( strs[2] );
        return new Integer[]{year, month, day};
    }

    // sample line: 6660,10417,2,#,11/06/2019 00:00:00,11/07/2019 00:00:00,39559605,4,21:00,
    private String extractDateFrom(String line) {
        return line.split(",")[4];
    }

    private String extractDateTo(String line) {
        return line.split(",")[5];
    }

    private java.util.List<TripData> enrichAlternateTripId(java.util.List<TripData> result) {
        return io.vavr.collection.Stream.ofAll(result)
                .groupBy(tripData -> tripData.routeId)
                .map(routeAndTrips -> {
                    String routeId = routeAndTrips._1;
                    io.vavr.collection.Stream<TripData> enrichedTrips = process(routeAndTrips._2);
                    return Tuple.of(routeId, enrichedTrips);
                    // could be shortened to Tuple.of(routeAndTrips._1, process(routeAndTrips._2))
                })
                .flatMap(tup -> tup._2)
                .toJavaList();
    }

    private io.vavr.collection.Stream<TripData> process(io.vavr.collection.Stream<TripData> allTrips) {
        List<TripData> tripsOfSunday = findTripsOfSunday(allTrips);
        List<Tuple2<String, List<TripData>>> tripsOfSundayByHour = tripsOfSunday.groupBy(tripData -> tripData.originalAimedDeparture).toList();
        List<Tuple2<String,String>> hourAndTripId = tripsOfSundayByHour.map(tuple -> Tuple.of(tuple._1, tuple._2.head().siriTripId)).toList();
        Map<String,String> hourToTripId = io.vavr.collection.HashMap.ofEntries(hourAndTripId);
        return allTrips
                .map(tripData -> {
                    String tdOfSundaySameHourTripId = hourToTripId.getOrElse(tripData.getOriginalAimedDeparture(), "");
                    tripData.alternateTripId = tdOfSundaySameHourTripId;
                    return tripData;
                });
    }


    private List<TripData> findTripsOfSunday(io.vavr.collection.Stream<TripData> tripsData) {
        return tripsData.filter(tripData -> "1".equals(tripData.dayOfWeek)).toList();
    }


    private boolean dayInWeekIs(String line, String calcDayInWeek) {
        return calcDayInWeek.equals(extractDayInWeek(line));
    }

    private String extractDayInWeek(String line) {
        // LineDetailRecordId,OfficeLineId,Direction,LineAlternative,FromDate,ToDate,TripId,DayInWeek,DepartureTime
        // typical line: 15531,10420,1,0,11/12/2018 00:00:00,01/01/2200 00:00:00,35460323,6,09:15,
        // in lines of tripIdToDate, dayInWeek has the values 1, 2, 3, 4, 5, 6, 7
        // and it is the 8th item in the line
        return line.split(",")[7];
    }

    private String calcDayInWeek(String date) {
        // date is in format 2019-05-31
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
        int val = dayOfWeek.getValue(); // this return 1-7, but 1 is Monday...
        // fix so that 1=Sunday, 2=Monday, ...
        int result = val + 1 ;
        if (result == 8) {
            result = 1;
        }
        return Integer.toString(result);
    }

    /**
     *
     * @param line      A line from the file TripIdToDate
     * @param dateOfTrip
     * @return
     */
    private TripData createTripData(String line, String dateOfTrip) {
        TripData td = new TripData();
        td.routeId = extractRouteIdFromTripIdToDateLine(line);
        td.date = dateOfTrip;
        //td.dayOfWeek = calcDayInWeek(dateOfTrip);
        td.dayOfWeek = extractDayInWeek(line);      // the real day of week written in this line
        td.siriTripId = extractTripIdIdFromTripIdToDateLine(line);
        //td.siri1 = thisTrip.map(line -> createSiriReading(line)).toJavaList();
//        td.siri = new SiriFeatureCollection(thisTrip.map(line -> createSiriPart(line)).asJava().toArray(new SiriFeature[]{}));
//        td.vehicleId = extractVehicleId(firstLine);
//        td.agencyCode = extractAgency(firstLine);
//        td.agencyName = agencyNameFromCode(td.agencyCode);
//        td.shortName = extractShortName(firstLine);
        td.originalAimedDeparture = extractAimedDepartureFromTripIdToDateLine(line);
//        td.suspicious = false;
//        if (td.siri.features.length < 20) {td.suspicious = true;};
        return td;
    }

    // sample line: 6660,10417,2,#,11/06/2019 00:00:00,11/07/2019 00:00:00,39559605,4,21:00,
    private String extractAimedDepartureFromTripIdToDateLine(String line) {
        return line.split(",")[8];
    }

    private String extractTripIdIdFromTripIdToDateLine(String line) {
        return line.split(",")[6];
    }

    private String extractRouteIdFromTripIdToDateLine(String line) {
        return line.split(",")[0];
    }

    /**
     * Find all the routeIds that appear in siri_rt_data files of the specified date
     * @param date
     * @return      A list of routeIds
     */
    public java.util.List<String> findAllBusLines(String date) {
        List<String> names = List.range(0, 20).map(i -> siriLogFilesDirectory + "siri_rt_data_v2." + date + "." + i + ".log.gz");  // 2019-03-31

        logger.warn("reading {} siri results log files for date {}", names.size(), date);

        // calc set of routeIds that appeared in Siri results on that day
        Stream<String> routes = this
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> gpsExists(line))
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


    public void compareTripsGtfsToSiri(String siriTrips, String gtfsTrips) {


    }

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

    public List<String> findAlternateTripIds1(final String routeId, final String date) {
        return findAlternateTripIds(routeId, date, List.empty(), HashMap.empty());
    }

    public List<String> findAlternateTripIds(final String routeId, final String date, final List<String> tripLinesIn, final Map<String, List<String>> serviceIdToCalendarLinesIn) {
        Map<String, List<String>> serviceIdToCalendarLinesTemp = serviceIdToCalendarLinesIn;
        //in trips.txt search: grep "^15532," trips.txt
        // (15532 is the routeId)
        logger.debug("searching trips.txt for trip IDs of route {} (lines in trips.txt that start with {})", routeId, routeId);
        List<String> tripLines = tripLinesIn;
        if ((tripLines == null) || tripLines.isEmpty()) {
            final String gtfsZipFileName = decideGtfsFileName(date);
            final String gtfsZipFileFullPath = Utils.findFile(directoryOfGtfsFile, gtfsZipFileName);
            tripLines = List.ofAll( tripFileReader.retrieveTripLinesFromFile(gtfsZipFileFullPath) );
        }
        final String prefixRoute = routeId + "," ;
        // from result, take unique serviceIds (the second value in each line)
        java.util.List<String> serviceIds = tripLines
                .filter(line -> line.startsWith(prefixRoute))
                .map(line -> getServiceId(line))
                .distinct()
                .sorted()
                .collect(Collectors.toList());
        logger.trace("found {} serviceIDs in trips.txt", serviceIds.size());
        if (!serviceIds.isEmpty()) {
            logger.trace("found these service IDs: {}", serviceIds);
        }
        if (serviceIds.isEmpty()) {
            logger.warn("no service IDs found!");
            return List.empty();
        }

        if ((serviceIdToCalendarLinesTemp == null) || serviceIdToCalendarLinesTemp.isEmpty()) {
            // for each serviceId, search: grep 19902 calendar.txt
            // (where 19902 is the serviceId)
            // result is (for example): 19902,0,0,1,1,1,0,0,20191015,20191019
            logger.trace("searching calendar file");
            serviceIdToCalendarLinesTemp = CalendarReader.make(directoryOfGtfsFile).readCalendar(date);
        }
        final Map<String, List<String>> serviceIdToCalendarLines = serviceIdToCalendarLinesTemp;

        // from all results, choose the one that:
        // a. date arg is inside date range (,20191015,20191019)
        // b. day of week of date arg has 1 in (,0,0,1,1,1,0,0,)
        // there should be only one such result.
        List<String> allLines = List.empty();
        for (String sId : serviceIds) {
            List<String> lines = serviceIdToCalendarLines.get(sId).get();
            allLines = allLines.appendAll( CalendarReader.linesContainDateAndDayOfWeek(lines.toList(), date) );
        }
        // expect list has only 1 line!!
        if (allLines.isEmpty()) {
            return List.empty();
        }
        final String serviceId = allLines.get(0).split(",")[0];
        final String prefixRouteAndService = routeId + "," + serviceId + ",";
        // (assuming the result has serviceId 19902) now search: grep "^15532,19902" trips.txt
        // now search in trips.txt lines with routeId, serviceId
        List<String> allTripsOfRouteAtDate = List.ofAll( tripLines
                .filter(line -> line.startsWith(prefixRouteAndService))
                .sorted()
                .collect(Collectors.toList())
        );
        // result is:
        /*
15532,19902,41569028_151019,,1,106264
15532,19902,41569029_151019,,1,106264
15532,19902,41569030_151019,,1,106264
15532,19902,41569031_151019,,1,106264
15532,19902,41569032_151019,,1,106264
15532,19902,41569033_151019,,1,106264
15532,19902,41569034_151019,,1,106264
15532,19902,41569035_151019,,1,106264
15532,19902,41569036_151019,,1,106264
15532,19902,41569037_151019,,1,106264
15532,19902,41569038_151019,,1,106264
15532,19902,41569039_151019,,1,106264
15532,19902,41569040_151019,,1,106264
15532,19902,41569041_151019,,1,106264
15532,19902,41569042_151019,,1,106264
15532,19902,41569043_151019,,1,106264
15532,19902,41569044_151019,,1,106264
15532,19902,41569045_151019,,1,106264
15532,19902,41569046_151019,,1,106264
         */
        List<String> allTripIds = allTripsOfRouteAtDate.map(line -> line.split(",")[2]);
        return allTripIds;
    }

    private Map<Integer, StopsTimeData> findAlternateTripId(final String routeId, final String originalAimedDeparture, final String date) {
        //in trips.txt search: grep "^15532," trips.txt
        // (15532 is the routeId)
        final String gtfsZipFileName = decideGtfsFileName(date);
        final String gtfsZipFileFullPath = Utils.findFile(directoryOfGtfsFile, gtfsZipFileName);
        final ReadZipFile rzf = new ReadZipFile();
        final String prefixRoute = routeId + "," ;
        // from result, take unique serviceIds (the second value in each line)
        java.util.List<String> serviceIds = List.ofAll(
                tripFileReader.retrieveTripLinesFromFile(gtfsZipFileFullPath))
                .filter(line -> line.startsWith(prefixRoute))
                .map(line -> getServiceId(line))
                .distinct()
                .sorted()
                .collect(Collectors.toList());
        // for each serviceId, search: grep 19902 calendar.txt
        // (where 19902 is the serviceId)
        // result is (for example): 19902,0,0,1,1,1,0,0,20191015,20191019
        CalendarReader calendarReader = new CalendarReader();
        final Map<String, List<String>> serviceIdToCalendarLines = calendarReader.readCalendar(date);

        // from all results, choose the one that:
        // a. date arg is inside date range (,20191015,20191019)
        // b. day of week of date arg has 1 in (,0,0,1,1,1,0,0,)
        // there should be only one such result.
        List<String> allLines = List.empty();
        for (String sId : serviceIds) {
            List<String> lines = serviceIdToCalendarLines.get(sId).get().toList();
            allLines.appendAll( calendarReader.linesContainDateAndDayOfWeek(lines, date) );
        }
        // expect list has only 1 line!!
        final String serviceId = allLines.get(0).split(",")[0];
        final String prefixRouteAndService = routeId + "," + serviceId + ",";
        // (assuming the result has serviceId 19902) now search: grep "^15532,19902" trips.txt
        // now search in trips.txt lines with routeId, serviceId
        List<String> allTripsOfRouteAtDate = List.ofAll(
                tripFileReader.retrieveTripLinesFromFile(gtfsZipFileFullPath))
                .filter(line -> line.startsWith(prefixRouteAndService))
                .sorted();
        // result is:
        /*
15532,19902,41569028_151019,,1,106264
15532,19902,41569029_151019,,1,106264
15532,19902,41569030_151019,,1,106264
15532,19902,41569031_151019,,1,106264
15532,19902,41569032_151019,,1,106264
15532,19902,41569033_151019,,1,106264
15532,19902,41569034_151019,,1,106264
15532,19902,41569035_151019,,1,106264
15532,19902,41569036_151019,,1,106264
15532,19902,41569037_151019,,1,106264
15532,19902,41569038_151019,,1,106264
15532,19902,41569039_151019,,1,106264
15532,19902,41569040_151019,,1,106264
15532,19902,41569041_151019,,1,106264
15532,19902,41569042_151019,,1,106264
15532,19902,41569043_151019,,1,106264
15532,19902,41569044_151019,,1,106264
15532,19902,41569045_151019,,1,106264
15532,19902,41569046_151019,,1,106264
         */
        List<String> allTripIds = allTripsOfRouteAtDate.map(line -> line.split(",")[2]);

        // so 41569028_151019, 41569029_151019, ... are the tripIds for that day
        // now you should match a OAD to each tripId:
        // for each tripId you search:
        // grep '^41569028_151019' stop_times.txt
        List<String> lines = stops.readStopTimesFile(allTripIds.toSet(), date);
        // and choose the line that ends with ",0"
        List<String> oadLines = lines.filter(line -> line.endsWith(",0")).toList();
        //41569028_151019,07:50:00,07:50:00,36782,1,0,1,0
        // the OAD is 07:50:00
        logger.debug("trips with OAD: {}", oadLines);
        return null;
    }

    private String getServiceId(String tripLine) {
        // sample line: 15532,19916,41268289_231019,xx,1,106264
        return tripLine.split(",")[1];
    }

    @Autowired
    Shapes shapesService;

    // uses GIS methods to calculate the distance between the 2 stops <from> and <to> on the shape of route <routeId>
    public String calcDistance(String routeId, String date, String[] from, String[] to, String precision, String method) {

        List<String> shape = shapesService.retrieveShape(routeId, date);
        //List.ofAll(shape).take(10).forEach(line -> logger.info(line));
        /*
2019-11-06 09:23:17.427  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712682,34.989084,1
2019-11-06 09:23:17.427  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712678,34.989091,2
2019-11-06 09:23:17.427  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712674,34.989100,3
2019-11-06 09:23:17.427  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712668,34.989119,4
2019-11-06 09:23:17.427  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712666,34.989128,5
2019-11-06 09:23:17.427  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712664,34.989144,6
2019-11-06 09:23:17.428  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712663,34.989154,7
2019-11-06 09:23:17.428  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712664,34.989166,8
2019-11-06 09:23:17.428  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712611,34.989236,9
2019-11-06 09:23:17.428  INFO 19596 --- [nio-8080-exec-2] org.hasadna.gtfs.service.SiriData        : 106873,31.712504,34.989342,10
         */
        List<Tuple2<Integer,String[]>> shapePoints =
                shape.map(line -> {
                            String[] s = line.split(",");
                            Tuple2<Integer,String[]> shapePoint = Tuple.of(Integer.parseInt(s[3]), new String[]{s[1], s[2]} );
                            return shapePoint;
                        });
        long dist = calcDist(from, to, shapePoints, precision, method);
        return Long.toString(dist);
    }

    /**
     *
     *
     * @param from  {lat, long} of starting point (in degrees)
     * @param to    {lat, long} of end point (in degrees)
     * @param shapePoints   the Shape of the route (a List of all coordinates)
     * @return distance in meters between 'from' and 'to' along the shape
     */
    private long calcDist(String[] from, String[] to, List<Tuple2<Integer, String[]>> shapePoints, String precision, String method) {
        // assuming that from is on the shape, lets find the point in the shapePoints that is the same as from
        int fromPoint = findPoint(from, shapePoints);
        int toPoint = findPoint(to, shapePoints);
        logger.debug("calc distance from point {} to point {}", fromPoint, toPoint);
        double result = calcDistBetween(fromPoint, toPoint, shapePoints, precision, method);
        logger.debug("d={}", result);
        return (new Double(result)).longValue();
    }

    private double calcDistBetween_orig(int fromPoint, int toPoint, List<Tuple2<Integer, String[]>> shapePoints) {
        double totalDist = 0.0;
        if (fromPoint > 0) {
            logger.debug("{}   {},{}     0.0", fromPoint - 1, shapePoints.get(fromPoint - 1)._2[0], shapePoints.get(fromPoint - 1)._2[1]);
        }
        for (int index = fromPoint; index <= toPoint; index++) {
            if (index == 0) {
                continue;
            }
            double d = distance2(shapePoints.get(index-1)._2, shapePoints.get(index)._2);
            logger.debug("{}   {},{}     {}", index, shapePoints.get(index)._2[0], shapePoints.get(index)._2[1], d);
            totalDist = totalDist + d;
        }
        return totalDist;
    }



    private double calcDistBetween(int fromPoint, int toPoint, List<Tuple2<Integer, String[]>> shapePoints, String precision, String method) {
        int precisionInMeters = Integer.parseInt(precision);
        double totalDist = 0.0;
        if (fromPoint > 0) {
            logger.debug("{}   {},{}     0.0", fromPoint - 1, shapePoints.get(fromPoint - 1)._2[0], shapePoints.get(fromPoint - 1)._2[1]);
        }
        int prev = fromPoint;
        for (int index = fromPoint+1; index <= toPoint; index++) {
            if (index == 0) {
                continue;
            }
            double d = 0.0;
            if ("2".equals(method)) {
                d = distance2(shapePoints.get(prev)._2, shapePoints.get(index)._2);
            }
            else {
                d = distance1(shapePoints.get(prev)._2, shapePoints.get(index)._2);
            }
            if (d < precisionInMeters) {
                // measure from prev to index+1
                logger.debug("SKIPPED  {}   {},{}     {}   (precision={})", index, shapePoints.get(index)._2[0], shapePoints.get(index)._2[1], d, precision);
            }
            else {
                totalDist = totalDist + d;
                logger.debug("{}   {},{}     {},   total {}", index, shapePoints.get(index)._2[0], shapePoints.get(index)._2[1], d, totalDist);
                prev = index;
            }
        }
        return totalDist;
    }




    // assuming z1 and z2 are very close, we use Pithagoras
    double distance1(String[] z1, String[] z2) {
        // convert degrees to meters
        double[] w1 = degreesToMeters(z1);
        double[] w2 = degreesToMeters(z2);
        double pow = Math.pow((w1[0] - w2[0]), 2) + Math.pow((w1[1] - w2[1]), 2);
        double result = Math.sqrt(pow);
        return result;
    }

    // using https://stackoverflow.com/questions/639695/how-to-convert-latitude-or-longitude-to-meters
    double distance2(String[] z1, String[] z2) {
        double lat1 = Double.parseDouble(z1[0]);
        double lat2 = Double.parseDouble(z2[0]);
        double lon1 = Double.parseDouble(z1[1]);
        double lon2 = Double.parseDouble(z2[1]);
        double latMid = (lat1+lat2 )/2.0;  // or just use Lat1 for slightly less accurate estimate


        double m_per_deg_lat = 111132.954 - 559.822 * Math.cos( 2.0 * latMid ) + 1.175 * Math.cos( 4.0 * latMid);
        double m_per_deg_lon = (3.14159265359/180 ) * 6367449 * Math.cos ( latMid );

        double deltaLat = lat1 - lat2;  //fabs(Lat1 - Lat2);
        double deltaLon = lon1 - lon2;  //fabs(Lon1 - Lon2);

        double dist_m = Math.sqrt (  Math.pow( deltaLat * m_per_deg_lat,2) + Math.pow( deltaLon * m_per_deg_lon , 2) );
        return dist_m;
    }

    // see https://stackoverflow.com/questions/639695/how-to-convert-latitude-or-longitude-to-meters
    private double[] degreesToMeters(String[] z) {
        double x = Double.parseDouble(z[0]);
        double y = Double.parseDouble(z[1]);
        double xm = Math.toRadians(x) * RADIUS_OF_EARTH;
        double ym = Math.toRadians(y) * RADIUS_OF_EARTH;
        return new double[]{xm, ym};
    }

    private int findPoint(String[] point, List<Tuple2<Integer, String[]>> shapePoints) {
        int numberOfCharactersToRemove = 0 ;
        int result = -1;
        while (numberOfCharactersToRemove < 4) {
            result = findPointN(point, shapePoints, numberOfCharactersToRemove);
            if (result >= 0) {
                break;
            }
            numberOfCharactersToRemove = numberOfCharactersToRemove + 1 ;
        }
        return result;
    }

    private int findPointN(String[] point, List<Tuple2<Integer, String[]>> shapePoints, int numberOfCharactersToRemove) {
        for (Tuple2<Integer, String[]> z : shapePoints) {
            if (sameAs(point[0], z._2()[0], numberOfCharactersToRemove) && sameAs(point[1],z._2()[1], numberOfCharactersToRemove) ) {
                return z._1();
            }
        }
        // not found!!!
        return -1;
    }

    private boolean sameAs(String s1, String s2, int numberOfCharactersToRemove) {
        String w1 = s1.substring(0, s1.length() - numberOfCharactersToRemove);
        String w2 = s2.substring(0, s2.length() - numberOfCharactersToRemove);
        return w1.equals(w2);
    }

}


