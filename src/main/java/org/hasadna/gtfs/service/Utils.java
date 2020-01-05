package org.hasadna.gtfs.service;

import io.vavr.collection.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Utils {

    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static String generateKey(String dataKind, String routeId, String date) {
        return dataKind + "$" + routeId + "@" + date;
    }

    public static String ensureFileExist(String gtfsZipFileFullPath) {
        String fileThatActuallyExists = gtfsZipFileFullPath;
        File f = new File(gtfsZipFileFullPath);
        while (!f.exists()) {
            logger.warn("file {} not found", f.getName());
            fileThatActuallyExists = findPreviousDate(fileThatActuallyExists);
            if (null == fileThatActuallyExists) {
                break;
            }
            f = new File(fileThatActuallyExists);
        }
        logger.info("using file {}", fileThatActuallyExists);
        return fileThatActuallyExists;
    }

    private static String findPreviousDate(final String gtfsZipFileFullPath) {
        String[] strs = gtfsZipFileFullPath.split(".zip")[0].split("-");
        String day = strs[strs.length - 1];
        String previousDay = Integer.toString( Integer.parseInt(day) - 1 );
        if ("0".equals(previousDay)) {
            return null;
        }
        if (previousDay.length() == 1) {
            previousDay = "0" + previousDay;
        }
        String ret = gtfsZipFileFullPath.split(day + ".zip")[0] + previousDay + ".zip";
        return ret;
    }

    public static String extractAimedDeparture(String line) {
        return line.split(",")[6];
    }




    public static String findFile(final String possibleDirectories, final String fileName) {
        List<String> dirs = List.of(possibleDirectories.split(File.pathSeparator));
        for (String dir : dirs) {
            final String fullPath = dir + File.separatorChar + fileName;
            if (Files.isRegularFile(Paths.get(fullPath))) {
                return fullPath;
            }
        }
        return null;
    }

    public static String createFilePath(final String possibleDirectories, final String fileName) {
        // TODO solve problem!!! dirs.get(0) is not where all GTFS files exist
        List<String> dirs = List.of(possibleDirectories.split(File.pathSeparator));
        if (dirs.isEmpty()) return fileName;
        return dirs.get(0) + File.separatorChar + fileName;
    }


    public static StopWatch stopwatchStart() {
        StopWatch sw = new StopWatch();
        sw.start();
        return sw;
    }

    public static String stopwatchStopInMillis(StopWatch sw) {
        sw.stop();
        return Long.toString(sw.getLastTaskTimeMillis());
    }

}
