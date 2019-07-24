package org.hasadna.gtfs.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Utils {

    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static String ensureFileExist(String gtfsZipFileFullPath) {
        String fileThatActuallyExists = gtfsZipFileFullPath;
        File f = new File(gtfsZipFileFullPath);
        while (!f.exists()) {
            logger.warn("file {} not found", f.getName());
            fileThatActuallyExists = findPreviousDate(fileThatActuallyExists);
            f = new File(fileThatActuallyExists);
        }
        logger.info("using file {}", fileThatActuallyExists);
        return fileThatActuallyExists;
    }

    private static String findPreviousDate(final String gtfsZipFileFullPath) {
        String[] strs = gtfsZipFileFullPath.split(".zip")[0].split("-");
        String day = strs[strs.length - 1];
        String previousDay = Integer.toString( Integer.parseInt(day) - 1 );
        if (previousDay.length() == 1) {
            previousDay = "0" + previousDay;
        }
        String ret = gtfsZipFileFullPath.split(day + ".zip")[0] + previousDay + ".zip";
        return ret;
    }

    public static String extractAimedDeparture(String line) {
        return line.split(",")[6];
    }
}
