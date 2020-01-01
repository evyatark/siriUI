package org.hasadna.gtfs.service;

import io.vavr.Tuple2;
import io.vavr.collection.*;
import io.vavr.control.Option;
import org.hasadna.gtfs.entity.GtfsStopTime;
import org.hasadna.gtfs.entity.StopData;
import org.hasadna.gtfs.entity.StopsTimeData;
import org.hasadna.gtfs.repository.GtfsStopTimesRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.stream.Collectors;


@Component
@Scope("prototype")
public class Stops {

    @Value("${gtfsZipFileDirectory}")
    public String gtfsZipFileDirFullPath = "";      // :/home/evyatar/logs/work/2019-04/gtfs/

    //public String gtfsZipFileName = "";

    private static Logger logger = LoggerFactory.getLogger(Stops.class);

    @Autowired
    StopsCache stopsCache;

    /**
     * return Map of stopId to StopData
     * @param gtfsZipFileName
     * @return
     */
    public Map<String, StopData> readStopDataFromFile(String gtfsZipFileName) {

        Map<String, StopData> map = HashMap.empty();

        Stream<String> lines = readStopsFile(gtfsZipFileName);
        List<String> sLines = lines.toList();

        //sLines.map( line -> StopData.extractFrom(line)).forEach(stopData -> map.put(stopData.stop_id, stopData));
        for (String line : sLines) {
            StopData stopData = StopData.extractFrom(line);
            map = map.put(stopData.stop_id, stopData);
        }
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
        return (new ReadZipFile()).stopLinesFromFile(gtfsZipFileFullPath);
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

    @Autowired
    GtfsStopTimesRepository gtfsStopTimesRepository;

    public Map<Integer, StopsTimeData> readStopsTimeDataOfTripFromDB(String tripId, String date) {
        logger.debug("readStopsTimeDataOfTripFromDB, tripId={}, date={}", tripId, date);
        java.util.List<GtfsStopTime> all = gtfsStopTimesRepository.findByDateAndTripId(date, tripId);
        logger.debug("findByDateAndTripId returned a list with {} items", all.size());
        Map<String, List<GtfsStopTime>> map = List.ofAll(all).groupBy(gtfsStopTime -> gtfsStopTime.getStopSequence());
        logger.debug("after grouping, map contains {} keys: {}", map.keySet().length(), map.keySet());
        Map<Integer, StopsTimeData> mmap = HashMap.empty();
        for (String sequenceStr : map.keySet()) {
            logger.trace("processing stop_sequence {}", sequenceStr);
            Integer sequence = Integer.parseInt(sequenceStr);
            Option<List<GtfsStopTime>> w = map.get(sequenceStr);
            if (w.isEmpty() || w.get().isEmpty()) {
                continue;
            }
            if (w.get().length() > 1) {
                logger.warn("more than 1 item in list. tripId={}, date={}, items=", tripId, date);
                for (GtfsStopTime gtfsStopTime : w.get()) {
                    logger.warn("{}", gtfsStopTime);
                }

            }
            GtfsStopTime gst = w.get().head();
            StopsTimeData std = new StopsTimeData(gst.getTripId(), gst.getArrivalTime(), gst.getDepartureTime(), gst.getStopId(), gst.getStopSequence(), gst.getDistance());
            logger.debug("StopsTimeData created for trip {}, stop sequence {}", std.trip_id, std.stop_sequence);
            // read stops.txt file from GTFS zip. return map of stopId to stopData (all data we have about this stop in GTFS)
            Map<String, StopData> stopsMap = this.getMapForDate(date);  // it calls readStopsMap(date) only once, after that data is cached
            std.stopData = stopsMap.get(std.stop_id).get();
            logger.debug("stopData added from stopsMap. StopData={}", std.stopData);
            mmap = mmap.put(sequence, std);
        }
        return mmap;
    }

    public Map<String, Map<Integer, StopsTimeData>> readStopsTimeDataOfTripFromFile1(Set<String> tripIds, String date) {
        logger.debug("readStopsTimeDataOfTripFromFile1, date={}, tripIds={}", date, tripIds);
        Map<String, Map<Integer, StopsTimeData>> result = HashMap.empty();
        for (String tripId : tripIds) {
            result = result.put(tripId, readStopsTimeDataOfTripFromDB(tripId, date));
        }
        return result;
    }
    /**
     * reads from the stops_time.txt file only the lines relevant to specific tripId.
     * creates a map with key=tripId and value= a map containing all stops of that trip.
     * The key of the second map is the stop-sequence (starts from 1 and increasing by 1 until last stop)
     * The value of the second map is the StopsTimeData object, containing all data about that stop.
     * @return
     */
    public Map<String, Map<Integer, StopsTimeData>> readStopsTimeDataOfTripFromFile3(Set<String> tripIds, String date) {
        logger.debug("readStopsTimeDataOfTripFromFile3, date={}, tripIds={}", date, tripIds);
        Map<String, Map<Integer, StopsTimeData>> map = HashMap.empty();
        // read stops.txt file from GTFS zip. return map of stopId to stopData (all data we have about this stop in GTFS)
        Map<String, StopData> stopsMap = this.getMapForDate(date);  // getMapForDate() is cached, will read stops.txt of each date only once.

        logger.debug("reading stops_time file, filtering for trips {} ...", tripIds.toList().sorted().collect(Collectors.joining(",")));
        String gtfsZipFileFullPath = findGtfsZipFile(date);
        Map<String, List<Long>> x = generateMapOfTextLines(gtfsZipFileFullPath);    // TODO cache
        // in following line, function is called with date. From that date it can deduce the GTFS file name.
//        List<String> lines = readStopTimesFile(tripIds, date);  // this reads "stop_times.txt" file (which is very big)
//        if ((lines != null) && !lines.isEmpty()) {
        if (!x.isEmpty()) {
            logger.debug("Done! map has {} tripIds for date {}", x.size(), date);

            // (only one path over lines, for all tripIds! should be faster)
            // x is the list of line numbers in stop_times.txt for all lines that refer to each tripId
            logger.debug("generate StopsTimeData for all trips (60 seconds!) ...");  //{}", tripIds.toList().sorted().toJavaList());
            map = generateStopsTimeDataForAllTrips(tripIds, x, stopsMap, gtfsZipFileFullPath);
            // map is supposed to contain only tripIds of the form 20001000, not 20001000_23102019.
            // So we change all our tripIds to this form also.
            if (logger.isWarnEnabled()) {   // TODO in 50% of the cases map is empty, so check it first
                final Map<String, Map<Integer, StopsTimeData>> map1 = map;
                Set<String> tripsNotFound = tripIds.map(tripId -> fixTripId(tripId)).filter(tripId -> !map1.containsKey(tripId));
                if (!tripsNotFound.isEmpty()) {
                    logger.warn("tripIds not found in GTFS: {}", tripsNotFound.toList().sorted().toJavaList());
                }
            }
            logger.debug(" ... completed");
        }
//        else if ((lines != null) && lines.isEmpty() && logger.isWarnEnabled()) {
//            logger.warn("no lines were found in GTFS file stop_times.txt for any of the tripIds. Try searching trips by aimedDepartureTime!");
//            logger.warn("tripIds: {}", tripIds);
//
//        }
        return map;
    }

    private String fixTripId(String tripId) {
        if (tripId.indexOf('_') > 0) {
            return tripId.split("_")[0];
        }
        else {
            return tripId;
        }
    }


    private Map<String, Map<Integer, StopsTimeData>> generateStopsTimeDataForAllTrips(Set<String> tripIds, Map<String, List<Long>> linesByTripId, Map<String, StopData> stopsMap, String gtfsZipFileFullPath) {

        Map<String, Map<Integer, StopsTimeData>> map = HashMap.empty();

        List<String> lines = List.empty();
        logger.info("generateStopsTimeDataForAllTrips - find lines for {} trips...", tripIds.size());
        Map<String, List<String>> m = getLinesOfTrips(tripIds.toList().sorted(),
                linesByTripId,   // this invocation might produce empty list (if exception inside)
                "stop_times.txt",
                gtfsZipFileFullPath);
        for (String tripId : tripIds) {
            lines = lines.appendAll(m.getOrElse(tripId, List.empty()));
        }
        logger.info("generateStopsTimeDataForAllTrips - find lines              ... completed, {} lines.", lines.size());
        for (String line : lines) {
            if (lineStartsWithOneOfTripIds(line, tripIds)) {    // this if will always be true, because "lines" was created with exactly the lines of those tripIds
                StopsTimeData std = StopsTimeData.extractFrom(line);
                std.stopData = stopsMap.get(std.stop_id).get();
                map = addToMap(std, map);
            }
        }
        logger.info("generateStopsTimeDataForAllTrips({} tripIds) => map of {} entries", tripIds.size(), map.size());
        return map;
    }

    private boolean lineStartsWithOneOfTripIds(String line, Set<String> tripIds) {
        String thisTripId = line.substring(0, line.indexOf('_'));
        String orThisTripId = line.split(",")[0];
        return tripIds.contains(thisTripId) || tripIds.contains(orThisTripId);
    }


    public Map<String, Map<Integer, StopsTimeData>> readStopsTimeDataOfTripFromFile2(Set<Tuple2<String, String>> trips, String date) {
        // in trips, each tuple is (tripId, aimedDepartureTime)
        Map<String, Map<Integer, StopsTimeData>> map = HashMap.empty();
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

    private Map<String, Map<Integer, StopsTimeData>> addToMap(StopsTimeData stopsTimeData, Map<String, Map<Integer, StopsTimeData>> map) {
//        StopData sd = stopsMap.get(stopsTimeData.stop_id);
//        stopsTimeData.stopData = sd;
        logger.debug("adding stopsTimeData of sequence {} in trip {} to map (containing {} trips)", stopsTimeData.stop_sequence, stopsTimeData.trip_id, map.keySet().size());
        String tripIdWithoutDatePart = stopsTimeData.trip_id.substring(0, stopsTimeData.trip_id.indexOf('_'));
        if (map.containsKey(tripIdWithoutDatePart)) {
            Map<Integer, StopsTimeData> mapOfStopsForThisTrip = map.get(tripIdWithoutDatePart).get();
            mapOfStopsForThisTrip = mapOfStopsForThisTrip.put(Integer.parseInt(stopsTimeData.stop_sequence), stopsTimeData);
            map = map.put(tripIdWithoutDatePart, mapOfStopsForThisTrip);
        }
        else {
            Map<Integer, StopsTimeData> mapOfStopsForThisTrip = HashMap.empty();
            mapOfStopsForThisTrip = mapOfStopsForThisTrip.put(Integer.parseInt(stopsTimeData.stop_sequence), stopsTimeData);
            map = map.put(tripIdWithoutDatePart, mapOfStopsForThisTrip);
        }
        return map;
    }

    /**
     *
     * @param trip_ids
     * @param date
     * @return map of tripId to a map. The inner map is a map of stopId to Stop record.
     */
    public Map<String, Map<Integer, StopsTimeData>> generateStopsMap1(Set<String> trip_ids, String date, boolean plusOldResult) {
        if (plusOldResult) {
            logger.info("readStopsTimeDataOfTripFromFile3 started...");
            Map<String, Map<Integer, StopsTimeData>> oldResult = readStopsTimeDataOfTripFromFile3(trip_ids, date);
            logger.info("when retrieving the data from GTFS files (not from DB), we get these tripIds: {}", oldResult.keySet().toJavaSet());
            oldResult.keySet().toJavaSet().forEach(tripId ->
                logger.debug("for trip {} got a StopTimeData for {} stop sequences", tripId, oldResult.get(tripId).get().keySet().length())
            );
        }
        Map<String, Map<Integer, StopsTimeData>> result = readStopsTimeDataOfTripFromFile1(trip_ids, date);
        result = removeEmptyMaps(result);
        return result;
    }

    private Map<String, Map<Integer, StopsTimeData>> removeEmptyMaps(final Map<String, Map<Integer, StopsTimeData>> map) {
        Map<Integer, StopsTimeData> emptyMap = HashMap.empty();
        Map<String, Map<Integer, StopsTimeData>> result = map;
        // remove empty maps
        List<String> keysToRemove = List.empty();
        for (String key : result.keySet()) {
            if (result.getOrElse(key, emptyMap).isEmpty()) {
                keysToRemove = keysToRemove.append(key);
            }
        }
        if (!keysToRemove.isEmpty()) {
            for (String key : keysToRemove) {
                result = result.remove(key);
            }
        }
        return result;
    }

    public Map<String, Map<Integer, StopsTimeData>> generateStopsMap2(Set<String> tripIds, String date ) {
        //tripIds.map(tripId -> readStopsTimeDataOfTripFromFile(tripId, date);
        return readStopsTimeDataOfTripFromFile1(tripIds, date);
    }

    public Map<String, Map<Integer, StopsTimeData>> generateStopsMap(Set<Tuple2<String, String>> trips, String date ) {
        return readStopsTimeDataOfTripFromFile2(trips, date);
    }

    // applicative cache for Stops data of each date (content of file "stops.txt" from GTFS)
    Map<String, Map<String, StopData>> stopsMapsForAllDates = HashMap.empty();

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
                return stopsMapsForAllDates.get(date).get();
            } else {
                stopsMapsForAllDates = stopsMapsForAllDates.put(date, readStopsMap(date));
                return stopsMapsForAllDates.get(date).get();
            }
        }
        finally {
            logger.info("                     ... Done");
        }
    }

    public static String decideGtfsFileName(String date) {
        return "gtfs" + date + ".zip";
    }

    private Map<String, List<Long>> generateMapOfTextLines(String gtfsFileFullPath) {
        try {
            logger.info("generateMapOfTextLines  {}   -  started...  (why is it not cached???)", gtfsFileFullPath);
            return stopsCache.generateMapOfTextLines(gtfsFileFullPath);
        }
        finally {
            logger.info("... done");
        }
        /*
        String FILE_NAME_INSIDE_GTFS_ZIP = "stop_times.txt";
        logger.warn("traversing file {} in GTFS {}, this can take 45 seconds!", FILE_NAME_INSIDE_GTFS_ZIP, gtfsFileFullPath);
//        String gtfsFileFullPath = "/home/evyatar/logs/gtfs/gtfs2019-10-16.zip";
        ReadZipFile rzf = new ReadZipFile();
        long count = 0 ;
        Map<String, List<Long>> map = HashMap.empty();
        Iterator<String> s = null;
        try {
            s = rzf.readZipFile(gtfsFileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
                    .drop(1)    //.take(1000000)
                    .map(line -> line.substring(0, line.indexOf(",")))
                    .iterator();
        } catch (IOException e) {
            logger.error("unhandled exception", e);
            return HashMap.empty();
        }
        while (s.hasNext()) {
            String tripId = s.next();
            map = map.put(tripId, map.getOrElse(tripId, List.empty()).append(count++));
            if (count%1000000 == 0) logger.info("{}", count);
        }
        logger.warn("traversing completed!");
        return map;
        */
    }

    private List<String> getLinesOfTrip1(final String tripId,
                                        final Map<String, List<Long>> mapOfTextLines,
                                        final String FILE_NAME_INSIDE_GTFS_ZIP,
                                        final String fileFullPath)  {
//        String FILE_NAME_INSIDE_GTFS_ZIP = "stop_times.txt";
//        String fileFullPath = "/home/evyatar/logs/gtfs/gtfs2019-10-16.zip";
        ReadZipFile rzf = new ReadZipFile();

        List<Long> list = mapOfTextLines.getOrElse(tripId, List.empty()).sorted();
        if (list.isEmpty()) {
            logger.warn("no lines found for trip {} in GTFS file stop_times.txt", tripId);
            return List.empty();
        }
        //logger.info("list of lines for trip {}: {}", tripId, list);
        long count = 0;
        List<String> lines = List.empty();
        Iterator<String> iter = null;
        try {
            iter = rzf.readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
                    .drop(1)
                    .iterator();
        } catch (IOException e) {
            logger.error("unhandled exception", e);
            return List.empty();
        }
        while (iter.hasNext()) {
            String line = iter.next();
            if (list.head() == count) {
                lines = lines.append(line);
                list = list.tail();
            }
            if (list.isEmpty()) break;
            count = count+1;
        }
        return lines;
    }

    /**
     * return a map that for each tripId as key, has as value the list of all text lines from stop_times.txt
     * that refer to that tripId
     * @param tripIds
     * @param mapOfTextLines
     * @param FILE_NAME_INSIDE_GTFS_ZIP
     * @param fileFullPath
     * @return
     */
    private Map<String, List<String>> getLinesOfTrips(final List<String> tripIds,
                                        final Map<String, List<Long>> mapOfTextLines,
                                        final String FILE_NAME_INSIDE_GTFS_ZIP,
                                        final String fileFullPath) {
        ReadZipFile rzf = new ReadZipFile();

        List<Long> listOfLineNumbersOfAllTripIds = tripIds.flatMap(tripId -> mapOfTextLines.getOrElse(tripId, List.empty())).sorted();
        if (listOfLineNumbersOfAllTripIds.isEmpty()) {
            logger.warn("no lines found in GTFS file stop_times.txt for any of these trips {}", tripIds.sorted().toJavaList());
            return HashMap.empty();
        }

        long counter = 0;
        Map<Long, String> lines = HashMap.empty();   // key - number of line in stop_times.txt. value - the line itself
        Iterator<String> iter = null;
        try {
            iter = rzf.readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
                    .drop(1)
                    .iterator();
        } catch (IOException e) {
            logger.error("unhandled exception", e);
            return HashMap.empty();
        }

        while (iter.hasNext()) {
            String line = iter.next();
            if (listOfLineNumbersOfAllTripIds.head() == counter) {
                lines = lines.put(counter, line);
                listOfLineNumbersOfAllTripIds = listOfLineNumbersOfAllTripIds.tail();
            }
            if (listOfLineNumbersOfAllTripIds.isEmpty()) break;
            counter = counter + 1;
        }

        Map<Long, String> finalLines = lines;
        Map<String, List<String>> mapTripIdToAllItsLinesInStopTimesFile = HashMap.empty();
        if (!lines.keySet().isEmpty()) {
            for (String tripId : tripIds) {
                // list of line numbers
                List<Long> list = mapOfTextLines.getOrElse(tripId, List.empty()).sorted();
                List<String> linesOfTrip = list
                        .map(lineNumber -> finalLines.getOrElse(lineNumber, ""))
                        .filter(line -> !StringUtils.isEmpty(line));
                mapTripIdToAllItsLinesInStopTimesFile = mapTripIdToAllItsLinesInStopTimesFile.put(tripId, linesOfTrip);
            }

            return mapTripIdToAllItsLinesInStopTimesFile;
        } else {
            return HashMap.empty();
        }
    }
}
