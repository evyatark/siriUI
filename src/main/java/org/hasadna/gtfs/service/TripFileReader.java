package org.hasadna.gtfs.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class TripFileReader {

    private static Logger logger = LoggerFactory.getLogger(TripFileReader.class);

    @Value("${tripIdToDate.ZipFileDirectory}")
    public String directoryOfMakatFile;


    /**
     * This will cache all (!) lines of trips.txt, which is 20MB.
     * Note that this method must be in a separate component (from the caller)
     * so the Caching will work.
     * @param gtfsZipFileFullPath
     * @return a List of all lines in the Shapes.txt file
     */
    @Cacheable("tripLinesFromGtfsFileByDate")
    public List<String> retrieveTripLinesFromFile(String gtfsZipFileFullPath) {
        logger.info("retrieving tripLinesFromGtfsFileByDate {} - should happen only once for each date!", gtfsZipFileFullPath);
        return new ReadZipFile().tripLinesFromFile(gtfsZipFileFullPath).toJavaList();
    }

    // for seeing cache statistics, see https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#production-ready-metrics-cache

    /**
     * This file is read many times when processing routes of a single date.
     * So caching it should improve performance
     * @param date
     * @return
     */
    @Cacheable("makatLinesFromGtfsTripIdToDateByDate")
    public io.vavr.collection.List<String> readMakatFile(String date) {
        logger.info("retrieving makatLinesFromGtfsTripIdToDateByDate {} - should happen only once for each date!", date);
        String makatZipFileName = "TripIdToDate" + date + ".zip";    // TripIdToDate2019-05-17.zip
        String makatZipFileFullPath = Utils.findFile(directoryOfMakatFile, makatZipFileName);
        if (makatZipFileFullPath == null) {
            logger.warn("could not fine file {}, path used was: {}", makatZipFileFullPath, directoryOfMakatFile);
        }
        return (new ReadZipFile()).makatLinesFromFile(makatZipFileFullPath).toList();
    }
}
