package org.hasadna.gtfs.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.collection.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class Routes {

    private static Logger logger = LoggerFactory.getLogger(Routes.class);

    @Value("${gtfsZipFileDirectory}")       // :/home/evyatar/logs/work/2019-04/gtfs/
    public String gtfsZipFileDirFullPath = "";




    @Cacheable("routeIdsByDate")
    public String allRoutesAsJson(String date) throws JsonProcessingException {
        List<RouteData> routes = allRoutesByDate(date);
        logger.info("found {} routes on date {}", routes.size(), date);

        logger.debug("converting to JSON...");
        ObjectMapper x = new ObjectMapper();
        String json = x.writeValueAsString(routes.toJavaList());
        logger.debug("                  ... Done");
        logger.trace(json.substring(0, Math.min(3000, json.length())));logger.info("return json (size={} characters)", json.length());
        return json;
    }

    public List<RouteData> allRoutesByDate(String date) {
        final String gtfsZipFileName = "gtfs" + date + ".zip";
        String gtfsZipFileFullPath = Utils.findFile(gtfsZipFileDirFullPath, gtfsZipFileName);
        if (null == gtfsZipFileFullPath) {
            gtfsZipFileFullPath = Utils.ensureFileExist(Utils.createFilePath(gtfsZipFileDirFullPath, gtfsZipFileName));
        }
        logger.debug("collect routes...");
        List<RouteData> routes = collectAllRoutes(gtfsZipFileFullPath);
        logger.debug("{} routes.", routes.size());
        return routes;
    }

//    private static String ensureFileExist(String gtfsZipFileFullPath) {
//        String fileThatActuallyExists = gtfsZipFileFullPath;
//        File f = new File(gtfsZipFileFullPath);
//        while (!f.exists()) {
//            logger.warn("file {} not found", f.getName());
//            fileThatActuallyExists = findPreviousDate(fileThatActuallyExists);
//            f = new File(fileThatActuallyExists);
//        }
//        logger.info("using file {}", fileThatActuallyExists);
//        return fileThatActuallyExists;
//    }
//
//    private static String findPreviousDate(final String gtfsZipFileFullPath) {
//        String[] strs = gtfsZipFileFullPath.split(".zip")[0].split("-");
//        String day = strs[strs.length - 1];
//        String previousDay = Integer.toString( Integer.parseInt(day) - 1 );
//        if (previousDay.length() == 1) {
//            previousDay = "0" + previousDay;
//        }
//        String ret = gtfsZipFileFullPath.split(day + ".zip")[0] + previousDay + ".zip";
//        return ret;
//    }

    @Cacheable("default")
    public String routesAsJson(List<String> onlyTheseRoutes, String date) throws JsonProcessingException {

        final String gtfsZipFileName = "gtfs" + date + ".zip";
        final String gtfsZipFileFullPath = gtfsZipFileDirFullPath + gtfsZipFileName;

        logger.info("collect routes...");
        List<RouteData> routes = collectRoutes(onlyTheseRoutes, gtfsZipFileFullPath);

        logger.info("converting to JSON...");
        ObjectMapper x = new ObjectMapper();
        String json = x.writeValueAsString(routes.toJavaList());
        logger.info("                  ... Done");
        return json;
    }

    // TODO maybe return a stream, not a list?
    public List<RouteData> collectRoutes(List<String> onlyTheseRoutes, String gtfsZipFileFullPath) {
        return (new ReadZipFile()).routesFromFile(gtfsZipFileFullPath)
                .filter(line -> onlyTheseRoutes.contains(extractRouteId(line)))
                .map(line -> new RouteData(extractRouteId(line),extractAgencyCode(line), extractShortName(line), extractFrom(line), extractDestination(line)))
                .toList();
    }

    public List<RouteData> collectAllRoutes(String gtfsZipFileFullPath) {
        return (new ReadZipFile()).routesFromFile(gtfsZipFileFullPath)
                .filter(line -> !"2".equals(extractAgencyCode(line)))   // 2 = Trains
                .map(line -> createRouteData(line))
                .toList();
    }

    private RouteData createRouteData(String line) {
        String routeId = extractRouteId(line);
        String agency = extractAgencyCode(line);
        String shortName = extractShortName(line);
        String from = extractFrom(line);
        String to =  extractDestination(line);
        return new RouteData(routeId, agency, shortName, from, to);
    }

    // extracts from lines of routeId
    private String extractRouteId(String line) {
        if (StringUtils.isEmpty(line)) {
            return "";
        }
        // route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_color
        return line.split(",")[0];
    }

    private String extractAgencyCode(String line) {
        if (StringUtils.isEmpty(line)) {
            return "";
        }
        // route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_color
        return line.split(",")[1];
    }

    private String extractShortName(String line) {
        if (StringUtils.isEmpty(line)) {
            return "";
        }
        // route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_color
        return line.split(",")[2];
    }

    private String extractFrom(String line) {
        return extractLongName(line)[0];
    }

    private String extractDestination(String line) {
        return extractLongName(line)[1];
    }

    private String[] extractLongName(String line) {
        String longName = line.split(",")[3];
        try {
            // format: xxx <-> yyy - zz
            // first remove zz:
            int ind2 = longName.lastIndexOf('-');
            String zz = longName.substring(ind2 + 1);
            String xy = longName.substring(0, ind2);
            String[] fromAndTo = xy.split("<->");
            String xx = fromAndTo[0];
            String yy = fromAndTo[1];
            return new String[]{xx, yy, zz};
        }
        catch (Exception ex) {
            // absorbing
            return new String[]{longName, longName, ""};
        }
    }

    public class RouteData {
        public String routeId;
        public String agencyCode;
        public String shortName;
        public String from;
        public String to;

        public RouteData(String routeId, String agencyCode, String shortName, String from, String to) {
            this.routeId = routeId;
            this.agencyCode = agencyCode;
            this.shortName = shortName;
            this.from = from;
            this.to = to;
        }

        public String getRouteId() {
            return routeId;
        }

        public void setRouteId(String routeId) {
            this.routeId = routeId;
        }

        public String getAgencyCode() {
            return agencyCode;
        }

        public void setAgencyCode(String agencyCode) {
            this.agencyCode = agencyCode;
        }

        public String getShortName() {
            return shortName;
        }

        public void setShortName(String shortName) {
            this.shortName = shortName;
        }

        public String getFrom() {
            return from;
        }

        public void setFrom(String from) {
            this.from = from;
        }

        public String getTo() {
            return to;
        }

        public void setTo(String to) {
            this.to = to;
        }
    }
}
