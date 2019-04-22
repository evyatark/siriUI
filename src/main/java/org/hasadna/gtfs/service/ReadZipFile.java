package org.hasadna.gtfs.service;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ReadZipFile {

    private static Logger logger = LoggerFactory.getLogger(ReadZipFile.class);

    public io.vavr.collection.Stream<String> readZipFileV(String filePath, String fileInsideZip) throws IOException {
        io.vavr.collection.Stream <String> vLines =
            readZipFile(filePath, fileInsideZip)
                    .collect(io.vavr.collection.Stream.collector());
        return vLines;
    }

    public Stream<String> readZipFile(String filePath, String fileInsideZip) throws IOException {
        final ZipFile file = new ZipFile(filePath);
        ZipEntry entry = findEntry(file.entries(), fileInsideZip);
        if (entry == null) return null;
        return readFromInputStream(file.getInputStream(entry));
    }


    public ZipEntry findEntry(Enumeration<? extends ZipEntry> entries, String fileInsideZip) {
        while (entries.hasMoreElements()) {
            final ZipEntry entry = entries.nextElement();
            if (entry.getName().equals(fileInsideZip)) {
                return entry;
            }
        }
        return null;
    }

    private Stream<String> readFromInputStream(InputStream is) {
        InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);
        return br.lines();
    }


    /****************************
     *
     *  Stops.txt
     *
     ****************************/

    public io.vavr.collection.Stream<String> stopLinesFromFile(String fileFullPath) {
        io.vavr.collection.Stream<String> lines = io.vavr.collection.Stream.empty();
        try {
            lines = readZipFileV(fileFullPath, "stops.txt")
                    .collect(io.vavr.collection.Stream.collector());
        } catch (Exception ex) {

        }
        return lines;
    }


    /****************************
     *
     *  Stop_Times.txt
     *
     ****************************/

    /**
     * return a (lazy!) Stream of all lines in stop_times.txt file inside the GTFS zip file.
     * client code using this method is expected to filter only a small portion of the lines
     * (for eaxmple, all lines of a specific trip id)
     * @param fileFullPath
     * @return
     */
    public io.vavr.collection.Stream<String> stopTimesLinesFromFile(String fileFullPath) {
        io.vavr.collection.Stream<String> lines = null;
        try {
            lines = readZipFileV(fileFullPath, "stop_times.txt")
                    .collect(io.vavr.collection.Stream.collector());
        } catch (Exception ex) {

        }
        return lines;
    }

    public io.vavr.collection.Stream<String> stopTimesLinesOfTripFromFile(String fileFullPath, String tripId) {
        io.vavr.collection.Stream<String> lines = null;
        try {
            lines = readZipFileV(fileFullPath, "stop_times.txt")
                    .filter(line -> line.startsWith(tripId + "_"))
                    .collect(io.vavr.collection.Stream.collector());
        } catch (Exception ex) {

        }
        return lines;
    }

    public Stream<String> stopTimesLinesOfTripFromFile1(String fileFullPath, String tripId) {
        String FILE_NAME_INSIDE_GTFS_ZIP = "stop_times.txt";
        logger.info("read file {} inside {}, filter trip {} ...", FILE_NAME_INSIDE_GTFS_ZIP, fileFullPath, tripId);
        Stream<String> lines = null;
        Set<String> tripsInThisFile = new HashSet<>();
        try {
            lines = readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
                    .filter(line -> {
                        String trip = line.split(",")[0];
                        if (!tripsInThisFile.contains(trip)) {
                            tripsInThisFile.add(trip);
                            logger.debug(trip);
                        }
                        if (line.startsWith(tripId.substring(0,6)))
                            logger.debug("{}: {}", tripId, line);
                        return line.startsWith(tripId + "_");
                    });
        } catch (Exception ex) {

        }
        logger.info("                             ...Done");
        logger.info("trips in this stop_times file: {}", tripsInThisFile);
        return lines;
    }

}