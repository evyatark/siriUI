package org.hasadna.gtfs.service;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import org.hasadna.gtfs.entity.StopData;
import org.hasadna.gtfs.entity.StopsTimeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Scope("prototype")
public class Stops {

    @Value("${gtfsZipFileDirectory}")
    public String gtfsZipFileDirFullPath = "";      // :/home/evyatar/logs/work/2019-04/gtfs/

    //public String gtfsZipFileName = "";

    private static Logger logger = LoggerFactory.getLogger(Stops.class);




    public Map<String, StopData> readStopDataFromFile(String gtfsZipFileName) {

        Map<String, StopData> map = new HashMap<>();

        Stream<String> lines = readStopsFile(gtfsZipFileName);

        lines.map( line -> StopData.extractFrom(line)).forEach(stopData -> map.put(stopData.stop_id, stopData));

        return map;

    }

    private Stream<String> readStopsFile(String gtfsZipFileName) {
        //String originalGtfsZipFileFullPath = gtfsZipFileDirFullPath + gtfsZipFileName;
        //String gtfsZipFileFullPath = Utils.ensureFileExist(originalGtfsZipFileFullPath);
        String gtfsZipFileFullPath = Utils.findFile(gtfsZipFileDirFullPath, gtfsZipFileName);
        if (null == gtfsZipFileFullPath) {
            gtfsZipFileFullPath = Utils.ensureFileExist(Utils.createFilePath(gtfsZipFileDirFullPath, gtfsZipFileName));
        }
        if (null == gtfsZipFileFullPath) {
            logger.warn("some problem! no GTFS file found in {}", gtfsZipFileDirFullPath);
            return Stream.empty();
        }
        return (new ReadZipFile()).stopLinesFromFile(gtfsZipFileFullPath).toJavaStream();
    }


//    private Stream<String> readRoutesFile() {
//        //String gtfsZipFileFullPath = gtfsZipFileDirFullPath + gtfsZipFileName;
//        String gtfsZipFileFullPath = Utils.findFile(gtfsZipFileDirFullPath, gtfsZipFileName);
//        if (null == gtfsZipFileFullPath) {
//            logger.warn("could not find GTFS file of date {}. Searched this path: {}", gtfsZipFileName, gtfsZipFileDirFullPath);
//            return Stream.empty();
//        }
//        return (new ReadZipFile()).stopLinesFromFile(gtfsZipFileFullPath).toJavaStream();
//    }
//
//    private io.vavr.collection.Stream<String> readStopTimesFile() {
//        //String gtfsZipFileFullPath = gtfsZipFileDirFullPath + gtfsZipFileName;
//        String gtfsZipFileFullPath = Utils.findFile(gtfsZipFileDirFullPath, gtfsZipFileName);
//        if (null == gtfsZipFileFullPath) {
//            logger.warn("could not find GTFS file of date {}. Searched this path: {}", gtfsZipFileName, gtfsZipFileDirFullPath);
//            return io.vavr.collection.Stream.empty();
//        }
//        return (new ReadZipFile()).stopTimesLinesFromFile(gtfsZipFileFullPath);
//    }

    /**
     * returns a Java8 Lazy Stream of lines
     * @param tripIds
     * @return
     */
    public List<String> readStopTimesFile(Set<String> tripIds, String date) {
        String gtfsZipFileFullPath = findGtfsZipFile(date);

        logger.info("reading file {}", gtfsZipFileFullPath);
        return (new ReadZipFile()).stopTimesLinesOfTripsFromFile(gtfsZipFileFullPath, tripIds);
    }

//    public List<String> readStopTimesFile2(Set<Tuple2<String, String>> trips, String date) {
//        String gtfsZipFileFullPath = findGtfsZipFile(date);
//
//        logger.info("reading file {}", gtfsZipFileFullPath);
//        return (new ReadZipFile()).stopTimesLinesOfTripsFromFile(gtfsZipFileFullPath, trips);
//    }

    private String findGtfsZipFile(String date) {
        String gtfsZipFileName = decideGtfsFileName(date);
        String gtfsZipFileFullPath = Utils.findFile(gtfsZipFileDirFullPath, gtfsZipFileName);
        if (null == gtfsZipFileFullPath) {
            gtfsZipFileFullPath = Utils.ensureFileExist(Utils.createFilePath(gtfsZipFileDirFullPath, gtfsZipFileName));
        }
        return gtfsZipFileFullPath;
    }


    /**
     * reads from the stops_time.txt file only the lines relevant to specific tripId.
     * creates a map with key=tripId and value= a map containing all stops of that trip.
     * The key of the second map is the stop-sequence (starts from 1 and increasing by 1 until last stop)
     * The value of the second map is the StopsTimeData object, containing all data about that stop.
     * @return
     */
    public Map<String, Map<Integer, StopsTimeData>> readStopsTimeDataOfTripFromFile(Set<String> tripIds, String date) {
        Map<String, java.util.Map<Integer, StopsTimeData>> map = new HashMap<>();
        // read stops.txt file from GTFS zip. return map of stopId to stopData (all data we have about this stop in GTFS)
        Map<String, StopData> stopsMap = this.getMapForDate(date);  // getMapForDate() is cached, will read stops.txt of each date only once.

        logger.debug("reading stops_time file, filtering for trips {} ...", tripIds);
        // in following line, function is called with date. From that date it can deduce the GTFS file name.
        List<String> lines = readStopTimesFile(tripIds, date);  // this reads "stop_times.txt" file (which is very big)
        if ((lines != null) && !lines.isEmpty()) {
            logger.debug("Done! read {} lines from file stop_times.txt", lines.size());

            // (only one path over lines, for all tripIds! should be faster)
            map = generateStopsTimeDataForAllTrips(tripIds, lines, stopsMap);
            if (logger.isWarnEnabled()) {
                final Map map1 = map;
                logger.warn("tripIds not found in GTFS: {}", tripIds.stream().filter(tripId -> !map1.containsKey(tripId)).collect(Collectors.joining(",")));
            }
        }
        else if ((lines != null) && lines.isEmpty() && logger.isWarnEnabled()) {
            logger.warn("no lines were found in GTFS file stop_times.txt for any of the tripIds. Try searching trips by aimedDepartureTime!");
            logger.warn("tripIds: {}", tripIds);

        }
        return map;
    }


    private Map<String, java.util.Map<Integer, StopsTimeData>> generateStopsTimeDataForAllTrips(Set<String> tripIds, List<String> lines, Map<String, StopData> stopsMap) {

        Map<String, java.util.Map<Integer, StopsTimeData>> map = new HashMap<>();

        for (String line : lines) {
            if (lineStartsWithOneOfTripIds(line, tripIds)) {
                StopsTimeData std = StopsTimeData.extractFrom(line);
                std.stopData = stopsMap.get(std.stop_id);
                addToMap(std, map);
            }
        }
        return map;
    }

    private boolean lineStartsWithOneOfTripIds(String line, Set<String> tripIds) {
        String thisTripId = line.substring(0, line.indexOf('_'));
        String orThisTripId = line.split(",")[0];
        return tripIds.contains(thisTripId) || tripIds.contains(orThisTripId);
    }


    public Map<String, Map<Integer, StopsTimeData>> readStopsTimeDataOfTripFromFile2(Set<Tuple2<String, String>> trips, String date) {
        // in trips, each tuple is (tripId, aimedDepartureTime)
        Map<String, java.util.Map<Integer, StopsTimeData>> map = new HashMap<>();
        Map<String, StopData> stopsMap = this.getMapForDate(date);

        logger.debug("reading stops_time file, filtering for trips {} (not implemented yet!...", trips);
        /**
        List<String> lines = readStopTimesFile2(trips, date);   // this reads "stop_times.txt" file (which is very big)

        // copied from readStopsTimeDataOfTripFromFile:
        if ((lines != null) && !lines.isEmpty()) {
            logger.debug("Done! read {} lines from file stop_times.txt", lines.size());

            // (only one path over lines, for all tripIds! should be faster)
            map = generateStopsTimeDataForAllTrips(tripIds, lines, stopsMap);
            if (logger.isWarnEnabled()) {
                final Map map1 = map;
                logger.warn("tripIds not found in GTFS: {}", trips.stream().filter(tripId -> !map1.containsKey(tripId)).collect(Collectors.joining(",")));
            }
        }
        else if ((lines != null) && lines.isEmpty() && logger.isWarnEnabled()) {
            logger.warn("no lines were found in GTFS file stop_times.txt for any of the aimedDepartureTime!");
            logger.warn("trips: {}", trips);

        }
         **/
        return map;
    }

    private void addToMap(StopsTimeData stopsTimeData, Map<String, Map<Integer, StopsTimeData>> map) {
//        StopData sd = stopsMap.get(stopsTimeData.stop_id);
//        stopsTimeData.stopData = sd;
        logger.debug("adding stopsTimeData of sequence {} in trip {} to map (containing {} trips)", stopsTimeData.stop_sequence, stopsTimeData.trip_id, map.keySet().size());
        String tripIdWithoutDatePart = stopsTimeData.trip_id.substring(0, stopsTimeData.trip_id.indexOf('_'));
        if (map.containsKey(tripIdWithoutDatePart)) {
            Map<Integer, StopsTimeData> mapOfStopsForThisTrip = map.get(tripIdWithoutDatePart);
            mapOfStopsForThisTrip.put(Integer.parseInt(stopsTimeData.stop_sequence), stopsTimeData);
            map.put(tripIdWithoutDatePart, mapOfStopsForThisTrip);
        }
        else {
            Map<Integer, StopsTimeData> mapOfStopsForThisTrip = new HashMap<>();
            mapOfStopsForThisTrip.put(Integer.parseInt(stopsTimeData.stop_sequence), stopsTimeData);
            map.put(tripIdWithoutDatePart, mapOfStopsForThisTrip);
        }
    }

    /**
     *
     * @param trip_ids
     * @param date
     * @return map of tripId to a map. The inner map is a map of stopId to Stop record.
     */
    public Map<String, Map<Integer, StopsTimeData>> generateStopsMap1(Set<String> trip_ids, String date ) {
        return readStopsTimeDataOfTripFromFile(trip_ids, date);

    }

    public Map<String, Map<Integer, StopsTimeData>> generateStopsMap(Set<Tuple2<String, String>> trips, String date ) {
        return readStopsTimeDataOfTripFromFile2(trips, date);
    }

    // applicative cache for Stops data of each date (content of file "stops.txt" from GTFS)
    Map<String, Map<String, StopData>> stopsMapsForAllDates = new HashMap<>();

    public Map<String, StopData> readStopsMap(final String date) {
        //this.gtfsZipFileDirFullPath = "/home/evyatar/logs/work/2019-04/gtfs/" ;
        String gtfsZipFileName = decideGtfsFileName(date);
        Map<String, StopData> stopsMap = this.readStopDataFromFile(gtfsZipFileName);
        return stopsMap;
    }

    public Map<String, StopData> getMapForDate(final String date) {
        try {
            logger.info("get stops map for {} ...", date);
            if (stopsMapsForAllDates.containsKey(date)) {
                return stopsMapsForAllDates.get(date);
            } else {
                stopsMapsForAllDates.put(date, readStopsMap(date));
                return stopsMapsForAllDates.get(date);
            }
        }
        finally {
            logger.info("                     ... Done");
        }
    }

    public static String decideGtfsFileName(String date) {
        return "gtfs" + date + ".zip";
    }

}
