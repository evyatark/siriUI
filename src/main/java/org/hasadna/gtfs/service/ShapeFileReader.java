package org.hasadna.gtfs.service;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class ShapeFileReader {

    /**
     * This will cache all (!) lines of shapes.txt, which is 250MB.
     * But it is worth the size when we retrieve many shapes from thge same date.
     * Note that this method must be in a separate component (from the caller)
     * so the Caching will work.
     * @param gtfsZipFileFullPath
     * @return a List of all lines in the Shapes.txt file
     */
    @Cacheable("shapeLinesFromGtfsFileByDate")
    public List<String> retrieveShapeLinesFromFile(String gtfsZipFileFullPath) {
        return new ReadZipFile().shapeLinesFromFile(gtfsZipFileFullPath).toJavaList();
    }
}
