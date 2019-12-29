package org.hasadna.gtfs.service;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import org.hasadna.gtfs.entity.RawData;
import org.hasadna.gtfs.repository.RawDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StopWatch;

import java.util.stream.Collectors;

@Component
public class ReadSiriRawData {

    private static Logger logger = LoggerFactory.getLogger(ReadSiriRawData.class);

    @Value("${siriGzFilesDirectory}")
    public String siriLogFilesDirectory;    // /home/evyatar/logs/data/

    @Autowired
    SiriData siriData;

    @Autowired
    RawDataRepository rawDataRepository;
/*
    public Stream<String> getByDateAndRoute(final String date, final String routeId) {

        java.util.List<RawData> list = rawDataRepository.findByDateAndRouteId(date, routeId);
        //return list.stream().map(rawData -> rawData.getSiriRawData());
        Stream<String> raw = List.ofAll(list).map(rawData -> rawData.getSiriRawData()).toStream();
        return raw;
    }

    public java.util.List<String> getBy(final String date, final String routeId) {
        return rawDataRepository.findByDateAndRouteIdOrdered(date, routeId);
    }


    // retrieve the data already grouped by trip
    public Map<String, io.vavr.collection.Stream<String>> getByDateAndRouteGroupedByTrip(final String date, final String routeId) {
        //Object obj = rawDataRepository.findByDateAndRouteIdGroupByTripId(date, routeId);
        //Object obj = rawDataRepository.getRawDataGroupByTripId(date, routeId);
        Object obj = rawDataRepository.findRawDataGrouped(date, routeId);
        if (obj instanceof Map) {
            Map map = ((Map) obj);
            logger.debug("map size: {}",  map.keySet().length());
        }
        return null;
    }


    public boolean existInDatabase(final String date, final String routeId) {
        return (null != rawDataRepository.findFirstByDateAndRouteId(date, routeId));
//        long countByDate = rawDataRepository.countByDateAndRouteId(date, routeId);
//        boolean result = (countByDate > 0);
//        logger.debug("counted {} rows of date {} and route {}", countByDate, date, routeId);
//        return result;
    }

    public boolean existInDatabase(final String date) {
        return (null != rawDataRepository.findFirstByDate(date));
//        long countByDate = rawDataRepository.countByDate(date);
//        boolean result = (countByDate > 0);
//        logger.debug("counted {} rows of date {}", countByDate, date);
//        return result;
    }

    @Transactional
    public long deleteAllOfDate(final String date) {
        //long countByDate = rawDataRepository.countByDate(date);
        boolean exist = (null != rawDataRepository.findFirstByDate(date));
        if (exist) {
            logger.info("deleting all lines of date {} that already exist in table!",  date);
        }
        else {
            logger.warn("no rows exist of date {} !!", date);
            return -1;
        }
        int countDeleted = rawDataRepository.deleteQueryByDate(date);  // faster than deleteByDate(date);
        if ((null == rawDataRepository.findFirstByDate(date))) {
            logger.info("deleted all {} lines of date {}", countDeleted, date);
        }
        else {
            logger.warn("deleted {} lines (out of ?) of date {}", countDeleted,  date);

        }
        return countDeleted;
    }

    public String readEverything(final String date) {
        displayCurrentState();
        // names: list of names of all siri_rt_data files from the specified date
        // (assumes we won't have more than 20 files of siri results in the same date)
        String fileName = "siri_rt_data_v2." + date + "." + 0 + ".log.gz";
        String fullPath = Utils.findFile(siriLogFilesDirectory, fileName);
        if (fullPath == null) {
            logger.warn("could not find file {} in path {}", fileName, siriLogFilesDirectory);
        } else {
            logger.warn("found file {} in path {}, full path is {}", fileName, siriLogFilesDirectory, fullPath);
        }
        List<String> namesOldFormat = List.range(0, 20).map(i -> Utils.findFile(siriLogFilesDirectory, "siri_rt_data." + date + "." + i + ".log.gz")).filter(s -> s != null);  // 2019-04-04
        List<String> names = List.range(0, 20).map(i -> Utils.findFile(siriLogFilesDirectory, "siri_rt_data_v2." + date + "." + i + ".log.gz")).filter(s -> s != null);  // 2019-04-04
        names = names.appendAll(namesOldFormat);
        StopWatch s = new StopWatch("read_and_save_to_DB");
        s.start();
        logger.debug("the files are: {}", names.toString());
        logger.info("reading {} siri results log files...", names.size());

        // lines/vLines: Stream/List of all lines from the siri_rt-data file(s) [of day {date}, that belong to route ROUTE_ID
        Stream<String> lines = siriData
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                //.filter(line -> gpsExists(line))
                //.filter(line -> routeId.equals(this.extractRouteId(line)))
                ;

        // actual reading from file happens here:
//        io.vavr.collection.Stream<String> vLines =
//                lines.collect(io.vavr.collection.Stream.collector());
        logger.info("start save to DB...");
        long previousCount = rawDataRepository.count();
        final int NUMBER_OF = 1000000;
        logger.info("preparing to write {} rows, in groups of {}", lines.size(), NUMBER_OF);
        long countByDate = rawDataRepository.countByDate(date);
        if (countByDate == lines.size()) {
            logger.info("{} lines of date {} already exist in table, skip writing!", lines.size(), date);
        }
//        else if (countByDate < lines.size()) {
//            logger.warn("{} rows already in table, but siri files have {} lines. Abort!", countByDate, lines.size());
//            // TODO maybe remove all lines from that date, then read and save all?
//        }
        else if (countByDate > 0) {
            logger.warn("{} rows already in table. Siri files have {} lines. Abort!", countByDate, lines.size());
            // TODO maybe remove all lines from that date, then read and save all?
        }
        else {      // countByDate == 0 ==> nothing in DB from that date, let's save to DB
            //logger.warn("skip saving!");
            saveToDB(date, lines.toList(), NUMBER_OF);
//            long mc = 0;   // count millions of lines
//            while (true) {
//                try {
//                    rawDataRepository.saveAll(lines.take(NUMBER_OF).map(line -> new RawData(line, extractRouteId(line), date, extractTripId(line))));
//                    logger.info("{} M", ++mc);
//                    if (lines.size() < NUMBER_OF) {
//                        logger.info("completed {} rows", mc * NUMBER_OF);
//                        break;
//                    }
//                    lines = lines.drop(NUMBER_OF);
//                } catch (Exception ex) {
//                    logger.error("exception", ex);
//                    logger.info("while trying to save the following {} lines:", NUMBER_OF);
//                    List x = List.ofAll(lines.take(NUMBER_OF));
//                    for (int i : Stream.range(0, x.size())) {
//                        logger.info("{}: {}", i, x.get(i));
//                    }
//                    break;
//                }
//            }
        }
        logger.info("completed reading {} siri results log files", names.size());
        s.stop();
        long currentCount = rawDataRepository.count();
        logger.info("completed inserting {} siri rows to DB in {} seconds.", currentCount - previousCount, s.getTotalTimeSeconds());
        logger.info("currently DB contains {} siri rows.", currentCount );
        return "dummy";
    }

    public String saveToDB(String date, List<String> lines, final int NUMBER_OF) {
        logger.info("saving {} lines to DB, date={} ...", lines.size(), date);
        long mc = 0;   // count millions of lines
        while (true) {
            try {
                rawDataRepository.saveAll(lines.take(NUMBER_OF).map(line -> new RawData(line, extractRouteId(line), date, extractTripId(line))));
                logger.info("{} M", ++mc);
                if (lines.size() < NUMBER_OF) {
                    logger.info("completed {} rows", mc * NUMBER_OF);
                    break;
                }
                lines = lines.drop(NUMBER_OF);
            } catch (Exception ex) {
                logger.error("exception", ex);
                logger.info("while trying to save the following {} lines:", NUMBER_OF);
                List x = List.ofAll(lines.take(NUMBER_OF));
                for (int i : Stream.range(0, x.size())) {
                    logger.info("{}: {}", i, x.get(i));
                }
                break;
            }
        }
        logger.info("saving to DB, date={} ... completed", date);
        return "dummy";
    }

    private void displayCurrentState() {
        logger.info("currently DB has {} siri rows", rawDataRepository.count() );
    }


    public String extractRouteId(String line) {
        String routeId = line.split(",")[3];
        if (logger.isTraceEnabled()) logger.trace("line for route {}", routeId);
        //listOfRoutes = listOfRoutes.add(routeId);
        return routeId;
    }

    public String extractTripId(String line) {
        return line.split(",")[5];
    }

*/
}