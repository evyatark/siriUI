package org.hasadna.gtfs.service;

import io.vavr.collection.List;
import io.vavr.collection.Stream;
import org.assertj.core.api.Assertions;
import org.hasadna.gtfs.entity.StopData;
import org.hasadna.gtfs.entity.StopsTimeData;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StopsTimeTest {

    private static Logger logger = LoggerFactory.getLogger(StopsTimeTest.class);

    @Autowired
    Stops stops;

/*
    @Test
    public void test1() throws IOException {
        final String TRIP_ID = "37203669";
        final String date = "2019-04-05" ;
        //Stops stops = new Stops("/home/evyatar/logs/work/2019-04/gtfs/" + "gtfs" + date + ".zip") ;
        logger.info("started...");

        stops.gtfsZipFileDirFullPath = "/home/evyatar/logs/work/2019-04/gtfs/" ;
        stops.gtfsZipFileName = "gtfs" + date + ".zip" ;
        java.util.Map<String, java.util.Map<Integer, StopsTimeData>> map = new HashMap<>();
        Map<String, Map<Integer, StopsTimeData>> stopsTimeMap = stops.readStopsTimeDataOfTripFromFile(List.of(TRIP_ID).toJavaSet(), map, date);

        logger.info("       ...completed");

        Assertions.assertThat(stopsTimeMap).containsKeys(TRIP_ID);
        Assertions.assertThat(stopsTimeMap.keySet().stream().anyMatch(key -> key.startsWith(TRIP_ID ))).isTrue();  //contains a Key that starts with TRIP_ID;
        Assertions.assertThat(stopsTimeMap.get(TRIP_ID)).containsKeys(1, 2, 3);
        StopsTimeData std = stopsTimeMap.get(TRIP_ID).get(1);
        logger.info(std.toString());

        Assertions.assertThat(std.distance).isEqualTo("0");
    }


    @Test
    public void test2() throws IOException {
        final String TRIP_ID = "37203669";
        final String date = "2019-04-05" ;
        logger.info("started...");

        Map<String, Map<Integer, StopsTimeData>> stopsTimeMap = generateStopsMap(TRIP_ID, date, "/home/evyatar/logs/work/2019-04/gtfs/");


        logger.info("       ...completed");

        Assertions.assertThat(stopsTimeMap).containsKeys(TRIP_ID);
        Assertions.assertThat(stopsTimeMap.keySet().stream().anyMatch(key -> key.startsWith(TRIP_ID ))).isTrue();  //contains a Key that starts with TRIP_ID;
        Assertions.assertThat(stopsTimeMap.get(TRIP_ID)).containsKeys(1, 2, 3);
        int size = stopsTimeMap.get(TRIP_ID).size();
        for (int i : Stream.rangeClosed(1, size)) {
            StopsTimeData std = stopsTimeMap.get(TRIP_ID).get(i);
            logger.info(std.toString());
        }

        //Assertions.assertThat(std.distance).isEqualTo("0");
    }

    private Map<String, Map<Integer, StopsTimeData>> generateStopsMap(String trip_id, String date, String gtfsDir) {
        //Stops stops = new Stops(gtfsDir + "gtfs" + date + ".zip") ;
        stops.gtfsZipFileName = "gtfs" + date + ".zip";
        stops.gtfsZipFileDirFullPath = gtfsDir;
        java.util.Map<String, java.util.Map<Integer, StopsTimeData>> map = new HashMap<>();
        java.util.Map<String, java.util.Map<Integer, StopsTimeData>> populatedMap = stops.readStopsTimeDataOfTripFromFile(List.of(trip_id).toJavaSet(), map, date);
        logger.info("map populated. size=", populatedMap.keySet().size());
        logger.info("keys={}", populatedMap.keySet());
        return populatedMap;
    }
*/
}
