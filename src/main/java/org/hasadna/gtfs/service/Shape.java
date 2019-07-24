package org.hasadna.gtfs.service;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class Shape {
    @Cacheable("shapeByIdAndDate")
    public List<String> retrieveShapeId(String shapeId, String gtfsZipFileFullPath) {
        return new ReadZipFile().shapeLinesFromFile(gtfsZipFileFullPath)
                .filter(line -> line.startsWith(shapeId))
                .collect(Collectors.toList()); // only the lines that start with shapeId as the first item
    }

}
