package org.hasadna.gtfs.service;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class TripFileReader {

    /**
     * This will cache all (!) lines of trips.txt, which is 20MB.
     * Note that this method must be in a separate component (from the caller)
     * so the Caching will work.
     * @param gtfsZipFileFullPath
     * @return a List of all lines in the Shapes.txt file
     */
    @Cacheable("tripLinesFromGtfsFileByDate")
    public List<String> retrieveTripLinesFromFile(String gtfsZipFileFullPath) {
        return new ReadZipFile().tripLinesFromFile(gtfsZipFileFullPath).toJavaList();
    }

    // for seeing cache statistics, see https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#production-ready-metrics-cache
}
