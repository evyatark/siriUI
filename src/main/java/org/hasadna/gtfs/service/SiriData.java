package org.hasadna.gtfs.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.Iterator;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.control.Option;
import org.hasadna.gtfs.Spark;
import org.hasadna.gtfs.entity.StopsTimeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class SiriData {

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

    @Value("${enable.gtfs.stops:true}")
    public boolean displayStops;

    @Value("${search.stops.in.gtfs:false}")
    public boolean searchGTFS ;

    @Autowired
    Stops stops;

    @Autowired
    SchedulesData schedulesData;

    @Autowired
    MemoryDB db;

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
    @Autowired
    Spark spark;

    @Cacheable("siriByRouteAndDay")
    public String dayResults(final String routeId, String date) {
//        if (spark.dayResultsOfAllRoutes.containsKey(routeId)) {
//            return spark.dayResultsOfAllRoutes.get(routeId);
//        }

        Map<String, io.vavr.collection.Stream<String>> trips = findAllTrips(routeId, date);

        java.util.List<TripData> tripsData = buildFullTripsData(trips, date, routeId);


        // now comes computation that looks at all trips of this date (and route)
        /*
        java.util.List<String> listOfSiriDepartures = tripsData.stream().map(tripData -> tripData.originalAimedDeparture).collect(Collectors.toList());
        java.util.List<String> schedules =  schedulesData.findAllSchedules(routeId, date);
        logger.warn("day results started: routeId={}, date={}, schedules={}", routeId, date, schedules);
        java.util.List<String> listOfMissingDepartures = schedules.stream().filter(departure -> !listOfSiriDepartures.contains(departure)).collect(Collectors.toList());
        logger.info("departures missing from siri data: {}", listOfMissingDepartures);
        // list of siri departures that were not planned by GTFS
        java.util.List<String> listOfExcessDepartures = listOfSiriDepartures.stream().filter(departure -> !schedules.contains(departure)).collect(Collectors.toList());
        logger.info("departures from siri data that are not planned in GTFS: {}", listOfExcessDepartures);
        // add GTFS missing to list that will be displayed
        for (String departure : listOfMissingDepartures) {
            tripsData.add(BuidTripDataForMissingDeparture(departure, date, routeId));
        }
        */
        final String json = convertToJson(tripsData);
        logger.debug("day results completed: routeId={}, date={}", routeId, date);
//        spark.dayResultsOfAllRoutes.putIfAbsent(routeId, json);
        return json;
    }

    private TripData BuidTripDataForMissingDeparture(String departure, String date, String routeId) {
        TripData tripData = new TripData();
        tripData.originalAimedDeparture = departure;
        tripData.suspicious = true;
        tripData.date = date;
        tripData.routeId = routeId;
        return tripData;
    }


    /**
     * Process siri_rt_data files of the specified date, to create a data structure of
     * all trips of the specified route, according to data we received from Siri.
     * @param routeId
     * @param date
     * @return
     */
    public Map<String, io.vavr.collection.Stream<String>> findAllTrips(final String routeId, final String date) {
        // names: list of names of all siri_rt_data files from the specified date
        // (assumes we won't have more than 20 files of siri results in the same date)
        String fileName = "siri_rt_data_v2." + date + "." + 0 + ".log.gz";
        String fullPath = Utils.findFile(siriLogFilesDirectory, fileName);
        if (fullPath == null) {
            logger.warn("could not find file {} in path {}", fileName, siriLogFilesDirectory);
        }
        else {
            logger.warn("found file {} in path {}, full path is {}", fileName, siriLogFilesDirectory, fullPath);
        }
        List<String> namesOldFormat = List.range(0, 20).map(i -> Utils.findFile(siriLogFilesDirectory, "siri_rt_data." + date + "." + i + ".log.gz")).filter(s -> s != null);  // 2019-04-04
        List<String> names = List.range(0, 20).map(i -> Utils.findFile(siriLogFilesDirectory, "siri_rt_data_v2." + date + "." + i + ".log.gz")).filter(s -> s != null);  // 2019-04-04
        names = names.appendAll(namesOldFormat);
        logger.debug("the files are: {}", names.toString());
        logger.info("reading {} siri results log files...", names.size());

        // lines/vLines: Stream/List of all lines from the siri_rt-data file(s) [of day {date}, that belong to route ROUTE_ID
        Stream<String> lines = this
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> gpsExists(line))
                .filter(line -> routeId.equals(this.extractRouteId(line)));

        // actual reading from file happens here:
        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());
        logger.info("completed reading {} siri results log files", names.size());

        logger.info("grouping by tripId...");

        // map of all trips of {routeId} at day {date}
        // key: tripId
        // value: Stream/List of all lines from the siri_rt-data file(s), that belong to this trip
        Map<String, io.vavr.collection.Stream<String>> trips = vLines.groupBy(line -> this.extractTripId(line));
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
        logger.warn("{} {} route {}:got {} trips, {} of them suspicious", dayOfWeek, date, routeId, trips.size(), trips.count(trip->trip._2.count(i->true)<28));

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
    public java.util.List<TripData> buildFullTripsData(Map<String,io.vavr.collection.Stream<String>> trips, String date, String routeId) {

        // only from Siri:
        java.util.List<TripData> tripsData = buildTripData(trips, date, routeId);
        if ((tripsData == null) || tripsData.isEmpty()) {
            logger.warn("WARNING: empty or null trips data!");
            return new ArrayList<>() ;
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
    private java.util.List<Tuple2<String, String>> findScheduledTrips(String date, String routeId) {
        ArrayList<Tuple2<String, String>> list = new ArrayList<>();
        list.add(Tuple.of("a", "b"));
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
    public String convertToJson(java.util.List<TripData> tripsData) {
        logger.info("converting to JSON...");
        try {
            ObjectMapper x = new ObjectMapper();
            String json = x.writeValueAsString(tripsData);
            logger.debug("{}",x.writerWithDefaultPrettyPrinter().writeValueAsString(tripsData));
            logger.info("                  ... Done");
            return json;
        } catch (JsonProcessingException e) {
            logger.error("exception during marshalling", e);
            return "[]";
        }
    }

    public void findGtfsDataForSingleTrip(final String siriTripId, java.util.Map<String, java.util.Map<Integer, StopsTimeData>> allStopsOfAllTrips) {
        java.util.Map<Integer, StopsTimeData> stopsTimeData = allStopsOfAllTrips.get(siriTripId);
        StopFeatureCollection stops = StopsTimeData.createFeatures(stopsTimeData);
    }

    public TripData enrichSingleTrip(TripData tripData, java.util.Map<String, java.util.Map<Integer, StopsTimeData>> allStopsOfAllTrips) {
        findGtfsDataForSingleTrip(tripData.siriTripId, allStopsOfAllTrips);
        // no need to put in td both stopsTimeData and stops - they contain exactly the same information
        // removing it will make the JSON much smaller!
        // - not needed: tripData.stopsTimeData = allStopsOfAllTrips.get(tripData.siriTripId);
        java.util.Map<Integer, StopsTimeData> temporaryStopsTimeData = allStopsOfAllTrips.get(tripData.siriTripId);
        tripData.stops = StopsTimeData.createFeatures(temporaryStopsTimeData);
        return tripData;
    }

    public java.util.List<TripData> enrichTripsWithDataFromGtfs(java.util.List<TripData> tripsData, final String date) {
        // tripsData is trips that we found in Siri. But sometimes these trip IDs are not found in GTFS???
        if (searchGTFS) {   // this part searches in GTFS files, which is very time consuming!
            // GTFS data is needed for displaying the stops in Leaflet widget UI
            // also - SIRI data might not contain some trips, if They were not actually executed! So we need to get the list of planned trips from GTFS
            // also - the Trip IDs that SIRI reports might not exist at all in GTFS(!) - this is probably because of a bug in GTFS
            // (They use same tripIDs for all week days, but it should be different trip IDs each day. The TripIdToDate file contains the correct data)
            // it is possible to get the list of planned trips from the schedule files of that day (if you have them)
            logger.info("reading data about stops, from GTFS file ...");
            Set<String> tripIds = findAllTripIds(tripsData);
            java.util.Map<String, java.util.Map<Integer, StopsTimeData>> all =
                    stops.generateStopsMap1(tripIds, date);

            if (all.isEmpty()) {
                logger.warn("trip ids not found in stops map of GTFS. Trying to find by aimedDepartureTime...");
                Set<Tuple2<String, String>> tripIdsWithAimedDepartureTimes = findAllTripIdsWithAimedDepartureTimes(tripsData);
                stops.generateStopsMap(tripIdsWithAimedDepartureTimes, date);
            }

            return tripsData.stream().map(tripData -> enrichSingleTrip(tripData, all)).collect(Collectors.toList());
//            tripsData.forEach(tripData -> tripData.stopsTimeData = all.get(tripData.siriTripId));
//            logger.info("                  ... stopsTimeData Done");
//            tripsData.forEach(tripData -> {
//                tripData.stops = StopsTimeData.createFeatures(tripData.stopsTimeData);
//            });
//            logger.info("                  ... stopsFeatureCollection Done");
        }
        return tripsData;
    }

    private void displaySuspiciousTrips( java.util.List<TripData> tripsData) {
        List<String> suspicious =
                List.ofAll(
                        tripsData.stream()
                                .filter(trip -> "true".equals( trip.suspicious))
                                .map(tripData -> tripData.siriTripId) //using vavr instead of Java List
                );
        suspicious.forEach(tripId -> {
            int numberOfSiriPoints = List.ofAll(tripsData.stream())
                    .filter(tripData -> tripData.siriTripId.equals(tripId))
                    .map(td -> td.siri.features.length).get(0);
            logger.info("trip {} is suspicious: has only {} GPS points", tripId, numberOfSiriPoints);
        });

        List<String> dnsTrips =
                List.ofAll(
                        tripsData.stream()
                                .filter(trip -> "true".equals( trip.dns))
                                .map(tripData -> tripData.siriTripId) //using vavr instead of Java List
                );
        logger.info("DNS trips:{}", dnsTrips);
    }


    private Set<String> findAllTripIds(java.util.List<TripData> tripsData) {
        Set<String> aimedTimesOfAllTrips = tripsData.stream().map(tripData -> tripData.originalAimedDeparture).collect(Collectors.toSet());
        Set<String> tripIds = tripsData.stream().map(tripData -> tripData.siriTripId).collect(Collectors.toSet());
        return tripIds;
    }

    private Set<Tuple2<String, String>> findAllTripIdsWithAimedDepartureTimes(java.util.List<TripData> tripsData) {
        Set<String> aimedTimesOfAllTrips = tripsData.stream().map(tripData -> tripData.originalAimedDeparture).collect(Collectors.toSet());
        Set<String> tripIds = tripsData.stream().map(tripData -> tripData.siriTripId).collect(Collectors.toSet());
        io.vavr.collection.Stream<String> vt = io.vavr.collection.Stream.ofAll(tripIds);
        io.vavr.collection.Stream<Tuple2<String, String>> tuples = vt.zip(io.vavr.collection.Stream.ofAll(aimedTimesOfAllTrips));
        Set<Tuple2<String, String>> tuplesAsSet = tuples.toList().toSet().toJavaSet();
        return tuplesAsSet;
    }



    /**
     * build POJO representation of trips from the data in the method arguments
     * @param trips
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
    public java.util.List<TripData> buildTripData(Map<String, io.vavr.collection.Stream<String>> trips, String date, String routeId) {
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();

        java.util.List<TripData> tripsAccordingToSiri =
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


        // should be like this:
        java.util.List<TripData> tripsAccordingToSiri1 =
                trips
                   .keySet()
                   .map(tripId -> buildTripData(tripId, trips.get(tripId).getOrElse(io.vavr.collection.Stream.empty()), date, routeId, dayOfWeek))
                   .toJavaList();

        Map<String, String> siriTrips = findSiriTrips(List.ofAll(tripsAccordingToSiri1));
        display(siriTrips);
        java.util.List<TripData> tripsAccordingToGtfsTripIdToDate = buildTripsFromTripIdToDate(routeId, date);
        Map<String, String> gtfsTrips = findSiriTrips(List.ofAll(tripsAccordingToGtfsTripIdToDate));
        display(gtfsTrips);

/*
        // this is a list of TripData according to TripIdToDate which is part of the GTFS (sort of)
        // In general the lists (tripsAccordingToSiri and tripsAccordingToGtfsTripIdToDate) should be compatible,
        // except:
        // 1. tripsAccordingToGtfsTripIdToDate may contain trips that are not found in siri (most probably because
        // no bus executed that trip) - we will tag these trips as "missing" (or DNS = Did Not Start)_
        // 2. trip IDs should be the same for all days of week
        // (as opposed to trip IDs from GTFS file trips.txt, which are identical only on Sundays...)
        java.util.List<TripData> missingInSiri = findMissing(tripsAccordingToSiri, tripsAccordingToGtfsTripIdToDate);

        tripsAccordingToSiri.addAll(missingInSiri);
        // TODO sort tripsAccordingToSiri again (by originalAimedDeparture)
*/
        java.util.List<TripData> sortedTripsAccordingToSiri = sort(tripsAccordingToSiri);
        sortedTripsAccordingToSiri = completeWithGtfsData(tripsAccordingToSiri, tripsAccordingToGtfsTripIdToDate);
        // at that point sortedTripsAccordingToSiri contains also tripData objects of trips that appears in GTFS but not in siri data

        //return tripsAccordingToSiri;
        return sortedTripsAccordingToSiri;
    }

    /**
     * find in gtfsTrips (by originAimedDeparture time) the trips that do not apear in siriTrips
     * and add them to the siri list (with indication that they are DNS in Siri)
     * @param tripsAccordingToSiri
     * @param tripsAccordingToGtfsTripIdToDate
     * @return
     */
    private java.util.List<TripData> completeWithGtfsData(java.util.List<TripData> tripsAccordingToSiri, java.util.List<TripData> tripsAccordingToGtfsTripIdToDate) {
        java.util.List<String> gtfsHours = tripsAccordingToGtfsTripIdToDate.stream().map(tripData -> tripData.getOriginalAimedDeparture()).collect(Collectors.toList());
        java.util.List<String> siriHours1 = tripsAccordingToSiri.stream().map(tripData -> tripData.getOriginalAimedDeparture()).collect(Collectors.toList());
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

        List<TripData> siri = List.ofAll(tripsAccordingToSiri);
        for (String hour : gtfsHoursNotInSiri) {
            TripData tr =
                List.ofAll(tripsAccordingToGtfsTripIdToDate)
                    .find(tripData -> hour.equals(tripData.getOriginalAimedDeparture()))
                    .map(tripData -> {
                        tripData.dns = true;
                        //tripData.suspicious = true; // TODO actually we would prefer having here an indication that trip DNS)
                        return tripData;
                    }).get();
            siri = siri.append(tr);
        }
        // sort again (by oad) and return
        return siri.sortBy(tripData -> tripData.getOriginalAimedDeparture()).toJavaList();
    }

    private void display(Map<String, String> siriTrips) {
        List<Tuple2<String, String>> sorted = siriTrips.toList().sorted();
        //logger.info("all trips: {}",siriTrips.keySet().toSortedSet().map(key -> "trip/" + key + "/" + siriTrips.get(key)).toJavaStream().collect(Collectors.joining(",")));
        logger.info("all trips: {}", siriTrips.toString());
    }


    // returns a Map of key=departureTime and value=tripId
    // tripId is taken from Siri data.
    private Map<String, String> findSiriTrips(List<TripData> allTripsOfDay) {
        Map<String,String> departureToTripId = io.vavr.collection.HashMap.ofEntries(
                    allTripsOfDay
                    .map(tripData -> Tuple.of(tripData.originalAimedDeparture, tripData.siriTripId))
                    .sortBy(tuple -> tuple._2)
        );
        return departureToTripId;
    }


    /**
     * Build TripData objects for all trips of that route and that day
     * @param trips     all lines in the siri logs that belong to each trip. can be retrieved by calling findAllTrips(routeId, date)
     * @param date
     * @param routeId
     * @return      list of all trips (according to Siri data) of the specified route on the specified date.
     */
    public java.util.List<TripData> buildTripsData(Map<String, io.vavr.collection.Stream<String>> trips, String date, String routeId) {
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
        java.util.List<TripData> tripsAccordingToSiri =
                trips
                        .keySet()
                        .map(tripId -> buildTripData(tripId, trips.get(tripId).getOrElse(io.vavr.collection.Stream.empty()), date, routeId, dayOfWeek))
                        .toJavaList();
        java.util.List<TripData> sortedTripsAccordingToSiri = sort(tripsAccordingToSiri);
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
    public TripData buildTripData(String tripId, io.vavr.collection.Stream<String> tripLinesInLog, String date, String routeId, DayOfWeek dayOfWeek) {
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

    private java.util.List<TripData> findMissing(java.util.List<TripData> tripsAccordingToSiri, java.util.List<TripData> tripsAccordingToGtfsTripIdToDate) {
        java.util.List<TripData> missingInSiri = new ArrayList<>();
        for (TripData titdTripData : tripsAccordingToGtfsTripIdToDate) {
            if (!containsByTripId(tripsAccordingToSiri, titdTripData)) {
                titdTripData.dns = true;
                missingInSiri.add(titdTripData);
            }
        }
        return missingInSiri;
    }

    private boolean containsByTripId(java.util.List<TripData> tripsAccordingToSiri, TripData titdTripData) {
        return tripsAccordingToSiri.stream().anyMatch(tripData -> tripData.siriTripId.equals(titdTripData.siriTripId));
    }

    /**
     * use TripIdToDate (from GTFS) to build trips for each route from the specified list.
     * This method saves time by reading TripIdToDate only once
     * @param routeIds
     * @param date
     * @return      a map, key=routeId, value=list of trips that this route has on the specified date
     */
    public java.util.Map<String, java.util.List<TripData>> buildTripsForAllRoutesFromTripIdToDate(java.util.List<String> routeIds, String date) {
        logger.info("start building all trips for {} routes {}", routeIds.size(), routeIds);
        // allTrips is a big list but still it is better to have all of it in memory
        // because we are going to use it for all routes now
        java.util.List<String> allTrips = readMakatFile(date).collect(Collectors.toList());
        java.util.Map<String, java.util.List<TripData>> tripsForAllRoutes = new HashMap<>();
        for (String routeId : routeIds) {
            // for each route, we build trips with the "allTrips" object (that was computed once)
            java.util.List<TripData> tripsData = buildTripsFromTripIdToDate(routeId, date, allTrips);
            tripsForAllRoutes.put(routeId, tripsData);
        }
        logger.info("completed building all trips for {} routes, map contains trips for {} routes", routeIds.size(), tripsForAllRoutes.keySet().size());
        return tripsForAllRoutes;
    }

    public java.util.List<TripData> buildTripsFromTripIdToDate(String routeId, String date, java.util.List<String> allTrips) {
        logger.debug("looking for TripIdToDate trips of route {} on {}", routeId, date);
        try {
            java.util.List<TripData> result =
                    allTrips.stream()
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
            return sortedResult.toJavaList();
        }
        catch (Exception ex) {
            logger.error("exception while reading from TripIdToDate. continue without that data", ex);
            return new ArrayList<>();
        }
    }

    public java.util.List<TripData> buildTripsFromTripIdToDate(String routeId, String date) {
        logger.info("looking for TripIdToDate trips of route {} on {}", routeId, date);
        try {
            Stream<String> allTrips = readMakatFile(date);
            java.util.List<String> all = allTrips.collect(Collectors.toList());
            allTrips = all.stream();
            java.util.List<TripData> result =
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
                    .collect(Collectors.toList());
            // TODO sort by td.originalAimedDeparture
            List<TripData> sortedResult = List.ofAll(result).sortBy(td -> td.originalAimedDeparture);
            // note that in this file we have only hh:mm - no date
            // might be a problem if some trips are after midnight
            //result = enrichAlternateTripId(result);
            logger.info("looking for TripIdToDate trips         ... Completed ({} trips)", result.size());
            return sortedResult.toJavaList();
        }
        catch (Exception ex) {
            logger.error("exception while reading from TripIdToDate. continue without that data", ex);
            return new ArrayList<>();
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


}


