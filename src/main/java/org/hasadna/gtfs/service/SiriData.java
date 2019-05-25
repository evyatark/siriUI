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
import org.springframework.util.StringUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class SiriData {

    private static Logger logger = LoggerFactory.getLogger(SiriData.class);

    /**
     * This is the directory with all GTFS files of a specific month.
     * (it is also possible to put all GTFS files of several/all months in the same directory)
     */
    @Value("${gtfsZipFileDirectory}")
    public String gtfsZipFileDirFullPath = "";          // :/home/evyatar/logs/work/2019-04/gtfs/

    @Value("${siriGzFilesDirectory}")
    public String siriLogFilesDirectory;    // /home/evyatar/logs/data/

    @Value("${tripIdToDate.ZipFileDirectory}")
    public String directoryOfMakatFile;

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
    @Cacheable("siriByRouteAndDay")
    public String dayResults(final String routeId, String date) {
        logger.warn("day results: routeId={}, date={}", routeId, date);

        Map<String, io.vavr.collection.Stream<String>> trips = findAllTrips(routeId, date);

        java.util.List<TripData> tripsData = buildFullTripsData(trips, date, routeId);

        final String json = convertToJson(tripsData);
        return json;

//        try {
//            logger.info("building json for all trips...");
//            DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
//            json = buildJson(trips, dayOfWeek, date, routeId);
//            // TODO call createReport(trips, dayOfWeek, date, routeId);
//        } catch (JsonProcessingException e) {
//            logger.error("", e);
//        }
//        logger.warn(json);
//        return json;
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
        List<String> names = List.range(0, 20).map(i -> siriLogFilesDirectory + "siri_rt_data." + date + "." + i + ".log.gz");  // 2019-04-04

        logger.warn("reading {} siri results log files", names.size());

        // lines/vLines: Stream/List of all lines from the siri_rt-data file(s) [of day {date}, that belong to route ROUTE_ID
        Stream<String> lines = this
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> !line.endsWith(",0,0"))
                .filter(line -> routeId.equals(this.extractRouteId(line)));

        logger.info("completed reading {} siri results log files", names.size());

        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());

        logger.info("grouping by tripId");

        // map of all trips of {routeId} at day {date}
        // key: tripId
        // value: Stream/List of all lines from the siri_rt-data file(s), that belong to this trip
        Map<String, io.vavr.collection.Stream<String>> trips = vLines.groupBy(line -> this.extractTripId(line));
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
        logger.warn("{} {} route {}:got {} trips, {} of them suspicious", dayOfWeek, date, routeId, trips.size(), trips.count(trip->trip._2.count(i->true)<28));

        return trips;
    }

    /**
     * build a representation of Siri results from the data in the method arguments
     * @param trips - map of all trips of {routeId} at day {date}
     *                  key: tripId
     *                  value: Stream/List of all lines from the siri_rt-data file(s), that belong to this trip
     *
     * @param date      exact date of the day that is processed. String, format 2019-05-25
     * @param routeId   routeId of the route that is processed
     * @return
     * @throws JsonProcessingException
     */
    private java.util.List<TripData> buildFullTripsData(Map<String,io.vavr.collection.Stream<String>> trips, String date, String routeId) {
        java.util.List<TripData> tripsData = buildTripData(trips, date, routeId);


        if ((tripsData == null) || tripsData.isEmpty()) {
            logger.warn("WARNING: emty or null trips data!");
            return new ArrayList<>() ;
        }

        displaySuspiciousTrips(tripsData);

        // without the GTFS reading, we have a nice JSON, we only miss trips that were planned but not executed at all!
        // for example 420 2019-03-31 8:20
        // To know times of all planned trips, we can use the schedule files - in this case "siri.schedule.16.Sunday.json.2019-03-31"
        // (or we can read GTFS files and calculate again. Which is a small addition to
        // reading the time consuming Stops data and times that we can do now if searchGTFS is true)

        tripsData = enrichTripsWithDataFromGtfs(tripsData, date);

        return  tripsData;
    }



    public String convertToJson(java.util.List<TripData> tripsData) {
        logger.info("converting to JSON...");
        try {
            ObjectMapper x = new ObjectMapper();
            String json = x.writeValueAsString(tripsData);
            //x.writerWithDefaultPrettyPrinter().writeValueAsString(tripsData);
            logger.info("                  ... Done");
            return json;
        } catch (JsonProcessingException e) {
            logger.error("exception during marshalling", e);
            return "[]";
        }
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
            java.util.Map<String, java.util.Map<Integer, StopsTimeData>> map = new HashMap<>();
            java.util.Map<String, java.util.Map<Integer, StopsTimeData>> all =
                    stops.generateStopsMap(tripIds, date, gtfsZipFileDirFullPath, map);

            tripsData.forEach(tripData -> tripData.stopsTimeData = all.get(tripData.siriTripId));
            logger.info("                  ... stopsTimeData Done");
            tripsData.forEach(tripData -> {
                tripData.stops = StopsTimeData.createFeatures(tripData.stopsTimeData);
            });
            logger.info("                  ... stopsFeatureCollection Done");
        }
        return tripsData;
    }

    private void displaySuspiciousTrips( java.util.List<TripData> tripsData) {
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
    }


    private Set<String> findAllTripIds(java.util.List<TripData> tripsData) {
        return tripsData.stream().map(tripData -> tripData.siriTripId).collect(Collectors.toSet());
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
    private java.util.List<TripData> buildTripData(Map<String, io.vavr.collection.Stream<String>> trips, String date, String routeId) {
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
        java.util.List<TripData> tripsAccordingToSiri =
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
/*
        java.util.List<TripData> tripsAccordingToGtfsTripIdToDate = buildTripsFromTripIdToDate(routeId, date);

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
        return tripsAccordingToSiri;
    }



    private Stream<String> readMakatFile(String date) {
        String makatZipFileName = "TripIdToDate" + date + ".zip";    // TripIdToDate2019-05-17.zip
        String makatZipFileFullPath = directoryOfMakatFile + File.separatorChar + makatZipFileName;
        return (new ReadZipFile()).makatLinesFromFile(makatZipFileFullPath);
    }

    private java.util.List<TripData> findMissing(java.util.List<TripData> tripsAccordingToSiri, java.util.List<TripData> tripsAccordingToGtfsTripIdToDate) {
        java.util.List<TripData> missingInSiri = new ArrayList<>();
        for (TripData titdTripData : tripsAccordingToGtfsTripIdToDate) {
            if (!containsByTripId(tripsAccordingToSiri, titdTripData)) {
                titdTripData.suspicious = true; // actually "DNS", not suspicious
                missingInSiri.add(titdTripData);
            }
        }
        return missingInSiri;
    }

    private boolean containsByTripId(java.util.List<TripData> tripsAccordingToSiri, TripData titdTripData) {
        return tripsAccordingToSiri.stream().anyMatch(tripData -> tripData.siriTripId.equals(titdTripData.siriTripId));
    }

    private java.util.List<TripData> buildTripsFromTripIdToDate(String routeId, String date) {
        logger.info("looking for TripIdToDate trips of route {} on {}", routeId, date);
        try {
            Stream<String> allTrips = readMakatFile(date);
            java.util.List<TripData> result =
                    allTrips
                    .filter(line -> line.startsWith(routeId + ","))
                    // now filter by dayInWeek
                    .filter(line -> dayInWeekIs(line, calcDayInWeek(date)))
                    .map(line -> {
                                logger.info(line);
                                return line;
                            })
                    .map(line -> createTripData(line, date))
                    .collect(Collectors.toList());
            // TODO sort by td.originalAimedDeparture
            // note that in this file we have only hh:mm - no date
            // might be a problem if some trips are after midnight
            logger.info("looking for TripIdToDate trips         ... Completed ({} trips)", result.size());
            return result;
        }
        catch (Exception ex) {
            logger.error("exception while reading from TripIdToDate. continue without that data", ex);
            return new ArrayList<>();
        }
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
        td.dayOfWeek = calcDayInWeek(dateOfTrip);
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
        List<String> names = List.range(0, 20).map(i -> siriLogFilesDirectory + "siri_rt_data." + date + "." + i + ".log.gz");  // 2019-03-31

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


