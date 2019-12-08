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

    public Stream<String> getByDateAndRoute(String date, String routeId) {
        java.util.List<RawData> list = rawDataRepository.findByDateAndRouteId(date, routeId);
        //return list.stream().map(rawData -> rawData.getSiriRawData());
        Stream<String> raw = List.ofAll(list).map(rawData -> rawData.getSiriRawData()).toStream();
        return raw;
    }

    public boolean existInDatabase(final String date, final String routeId) {
        long countByDate = rawDataRepository.countByDateAndRouteId(date, routeId);
        boolean result = (countByDate > 0);
        logger.debug("counted {} rows of date {} and route {}", countByDate, date, routeId);
        return result;
    }

    public boolean existInDatabase(final String date) {
        long countByDate = rawDataRepository.countByDate(date);
        boolean result = (countByDate > 0);
        logger.debug("counted {} rows of date {}", countByDate, date);
        return result;
    }

    @Transactional
    public long deleteAllOfDate(final String date) {
        long countByDate = rawDataRepository.countByDate(date);
        if (countByDate > 0) {
            logger.info("deleting all {} lines of date {} that already exist in table!", countByDate, date);
        }
        else {
            logger.warn("no rows exist of date {} !!", date);
            return -1;
        }
        long countDeleted = rawDataRepository.deleteQueryByDate(date);  // faster than deleteByDate(date);
        if (countByDate == countDeleted) {
            logger.info("deleted all {} lines of date {}", countDeleted, date);
        }
        else {
            logger.warn("deleted {} lines (out of {}) of date {}", countDeleted, countByDate, date);

        }
        return countDeleted;
    }

    public void readEverything(final String date) {
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
        }
        logger.info("completed reading {} siri results log files", names.size());
        s.stop();
        long currentCount = rawDataRepository.count();
        logger.info("completed inserting {} siri rows to DB in {} seconds.", currentCount - previousCount, s.getTotalTimeSeconds());
        logger.info("currently DB contains {} siri rows.", currentCount );
/*
2019-12-04 09:47:58.326  INFO 20884 --- [nio-8080-exec-1] o.hasadna.gtfs.service.ReadSiriRawData   : reading 3 siri results log files...
2019-12-04 09:47:58.346  INFO 20884 --- [nio-8080-exec-1] o.hasadna.gtfs.service.ReadSiriRawData   : completed reading 3 siri results log files
2019-12-04 09:49:01.483  INFO 20884 --- [nio-8080-exec-1] o.hasadna.gtfs.service.ReadSiriRawData   : completed inserting 3 siri results log files to DB
2019-12-04 09:49:03.309  INFO 20884 --- [nio-8080-exec-1] o.h.gtfs.controller.GtfsController       : for date 2019-11-30 we have in DB 1434630 lines

 */
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


}