package org.hasadna.gtfs.service;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;

@Service
public class StreamResults {

    private static Logger logger = LoggerFactory.getLogger(StreamResults.class);

    @Value("${siriGzFilesDirectory:/tmp/}")
    String siriGzFilesDirectory;

    @Value("${date.of.results:2019-10-07}")
    String date;

    @Autowired
    SiriData siriData;

    public void do1() {
        List<String> routeIds = decideRouteIds();
        String routeId = routeIds.head();
        Instant start = Instant.now();

        Map<String, Stream<String>> results = findSiriLinesOfRouteByTrip(routeId, date);

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).getSeconds();
        displayResults(routeId, results, date, timeElapsed);
    }

    private void displayResults(String routeId, Map<String, Stream<String>> results, final String date, long timeInSeconds) {
        logger.info("For route {} on date {} found {} trips:", routeId, date, results.keySet().size());
        logger.info("The trip Ids are: {}", results.keySet().toString());
        logger.info("it took me {} seconds", timeInSeconds);
        for (String tripId : results.keySet()) {
            Stream<String> stream = results.get(tripId).getOrElse(Stream.empty());

            logger.info("for trip {} got {} lines", tripId, stream.size());
            logger.debug("here they are:");
            stream.forEach(line -> {
                //logger.debug(line);
                List<String> items = selectItems(line, List.of(8, 9, 10, 11));
                //logger.debug(items.toString());
                logger.debug(converftToJsonHardCoded(items));
            });
        }
        logger.info("done");
    }

    private String converftToJsonHardCoded(List<String> items) {
        String json = "{\"timestamp\":\"" + items.get(0) + "\",\"ea\":\"" + items.get(1) + "\",\"lat\":\"" + items.get(2) + "\",\"lon\":\"" + items.get(3) + "\"}";
        return json;
    }

    private List<String> selectItems(String line, List<Integer> positions) {
        // I assume positions is sorted
        String[] sp = line.split(",");
        List<String> result = positions.map(p -> sp[p]);
        return result;
    }

    private Map<String, Stream<String>> findSiriLinesOfRouteByTrip(final String routeId, final String date) {
        //List<String> dirs = List.of(siriGzFilesDirectory.split(":"));
        Map<String, Stream<String>> allTripsOfRoute = siriData.findAllTrips(routeId, date);
        return allTripsOfRoute;
    }

    private List<String> decideRouteIds() {
        return List.of("15531");    // line 420 BS to Jer
    }


    public void do2() {

    }

}
