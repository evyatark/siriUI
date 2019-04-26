package org.hasadna.gtfs.service;

import org.hasadna.gtfs.entity.StopData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

public class GlobalStopsMap {

    // moved to Stops.java
/*
    private static Logger logger = LoggerFactory.getLogger(GlobalStopsMap.class);

    @Autowired
    Stops stops;

    Map<String, Map<String, StopData>> stopsMapsForAllDates = new HashMap<>();

    public Map<String, StopData> readStopsMap(final String date) {
        //Stops stops = new Stops("/home/evyatar/logs/work/2019-03/gtfs/" + "gtfs" + date + ".zip") ;
        stops.gtfsZipFileDirFullPath = "/home/evyatar/logs/work/2019-03/gtfs/" ;
        stops.gtfsZipFileName = "gtfs" + date + ".zip" ;
        Map<String, StopData> stopsMap = stops.readStopDataFromFile();
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
*/
}
