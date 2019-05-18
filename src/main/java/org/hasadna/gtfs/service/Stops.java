package org.hasadna.gtfs.service;

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

    @Value("${gtfsZipFileFullPath:/home/evyatar/logs/work/2019-04/gtfs/}")
    public String gtfsZipFileDirFullPath = "";

    @Value("${gtfsZipFileName:gtfs2019-04-18.zip}")
    public String gtfsZipFileName = "";

    private static Logger logger = LoggerFactory.getLogger(Stops.class);




    public Map<String, StopData> readStopDataFromFile() {

        Map<String, StopData> map = new HashMap<>();

        Stream<String> lines = readStopsFile();

        lines.map( line -> StopData.extractFrom(line)).forEach(stopData -> map.put(stopData.stop_id, stopData));

        return map;

    }

    private Stream<String> readStopsFile() {
        String gtfsZipFileFullPath = gtfsZipFileDirFullPath + gtfsZipFileName;
        return (new ReadZipFile()).stopLinesFromFile(gtfsZipFileFullPath).toJavaStream();
    }


    private Stream<String> readRoutesFile() {
        String gtfsZipFileFullPath = gtfsZipFileDirFullPath + gtfsZipFileName;
        return (new ReadZipFile()).stopLinesFromFile(gtfsZipFileFullPath).toJavaStream();
    }




    private io.vavr.collection.Stream<String> readStopTimesFile() {
        String gtfsZipFileFullPath = gtfsZipFileDirFullPath + gtfsZipFileName;
        return (new ReadZipFile()).stopTimesLinesFromFile(gtfsZipFileFullPath);
    }

    /**
     * returns a Java8 Lazy Stream of lines
     * @param tripIds
     * @return
     */
    public List<String> readStopTimesFile(Set<String> tripIds) {
        String gtfsZipFileFullPath = gtfsZipFileDirFullPath + gtfsZipFileName;
        logger.info("reading file {}", gtfsZipFileFullPath);
        return (new ReadZipFile()).stopTimesLinesOfTripsFromFile(gtfsZipFileFullPath, tripIds);
    }


    /**
     * reads from the stops_time.txt file only the lines relevant to specific tripId.
     * creates a map with key=tripId and value= a map containing all stops of that trip.
     * The key of the second map is the stop-sequence (starts from 1 and increasing by 1 until last stop)
     * The value of the second map is the StopsTimeData object, containing all data about that stop.
     * @return
     */
    public Map<String, Map<Integer, StopsTimeData>> readStopsTimeDataOfTripFromFile(Set<String> tripIds, Map<String, Map<Integer, StopsTimeData>> map, String date) {

        // read stops.txt file from GTFS zip. return map of stopId to stopData (all data we have about this stop in GTFS)
        Map<String, StopData> stopsMap = this.getMapForDate(date);

        logger.debug("reading stops_time file, filtering for trips {} ...", tripIds);
        List<String> lines = readStopTimesFile(tripIds);
//        if (lines == null) {
//            logger.error("no lines found in stops_time file for trip {}", tripId);
//        }
//        else {
            logger.debug("Done! read {} lines from file stop_times.txt", lines.size());
//        }
//        lines = readStopTimesFile(tripId);  // read again because we want to return a stream
//        logger.info("                                               ...Done");
        if (lines != null) {

            // for each tripId from tripIds:
            for (String tripId : tripIds) {

                logger.info("extracting from lines of trip {}", tripId);
                //List<String> tripLines = lines.collect(Collectors.toList());
                //logger.info("got {} lines for trip {}", tripLines.size(), tripId);
                //lines = tripLines.stream();
                lines
                    .filter(line -> line.startsWith(tripId + "_"))
                    .map(line -> {
                            logger.debug(line);
                            return StopsTimeData.extractFrom(line);
                        })
                    .map(stopsTimeData -> {
                            stopsTimeData.stopData = stopsMap.get(stopsTimeData.stop_id);
                            return stopsTimeData;
                        })
                    .forEach(stopsTimeData -> addToMap(stopsTimeData, map));
                if (!map.containsKey(tripId)) {
                    logger.warn("tripId {} not found in GTFS!", tripId);
                }
            }
        }
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
     * @param gtfsDir
     * @param map
     * @return map of tripId to a map. The inner map is a map of stopId to Stop record.
     */
    public Map<String, Map<Integer, StopsTimeData>> generateStopsMap(Set<String> trip_ids, String date, String gtfsDir, Map<String, Map<Integer, StopsTimeData>> map ) {
        //Stops stops = new Stops(gtfsDir + "/" + "gtfs" + date + ".zip") ;
        this.gtfsZipFileDirFullPath = gtfsDir + "/";
        this.gtfsZipFileName = "gtfs" + date + ".zip";
        return readStopsTimeDataOfTripFromFile(trip_ids, map, date);
    }




    Map<String, Map<String, StopData>> stopsMapsForAllDates = new HashMap<>();

    public Map<String, StopData> readStopsMap(final String date) {
        this.gtfsZipFileDirFullPath = "/home/evyatar/logs/work/2019-04/gtfs/" ;
        this.gtfsZipFileName = "gtfs" + date + ".zip" ;
        Map<String, StopData> stopsMap = this.readStopDataFromFile();
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

}
