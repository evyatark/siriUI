package org.hasadna.gtfs.service;

import io.vavr.collection.HashMap;
import io.vavr.collection.Iterator;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class StopsCache {

    private static Logger logger = LoggerFactory.getLogger(StopsCache.class);

    @Cacheable("stopTimesTextLineMap")
    public Map<String, List<Long>> generateMapOfTextLines(String gtfsFileFullPath) {
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
        logger.warn("trips found (keys in map): {}", map.keySet().size());
        return map;
    }
}
