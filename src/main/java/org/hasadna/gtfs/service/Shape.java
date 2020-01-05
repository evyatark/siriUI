package org.hasadna.gtfs.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class Shape {

    @Autowired
    public ShapeFileReader shapeFileReader;

    public List<String> retrieveShapeId(String shapeId, String gtfsZipFileFullPath) {

        // This call is cached after the first time (for the same date)
        List<String> allLinesInFile = shapeFileReader.retrieveShapeLinesFromFile(gtfsZipFileFullPath);

        // so after the first call, the following pass over the list of lines is the only processing done.
        // And it should be much quicker than reading from the file.
        return allLinesInFile
                .stream()
                .filter(line -> line.startsWith(shapeId))
                .collect(Collectors.toList()); // only the lines that start with shapeId as the first item
    }

}
