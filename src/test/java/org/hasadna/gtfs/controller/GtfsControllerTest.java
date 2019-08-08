package org.hasadna.gtfs.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vavr.Function1;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import org.assertj.core.api.Assertions;
import org.hasadna.gtfs.service.Routes;
import org.hasadna.gtfs.service.SiriData;
import org.hasadna.gtfs.service.TripData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GtfsControllerTest {

    private static Logger logger = LoggerFactory.getLogger(GtfsControllerTest.class);

    @Autowired
    Routes routes;

    @Autowired
    GtfsController gtfsController;

    @Autowired
    SiriData siriData;

    List<String> routeIdsBS = Arrays.asList(
             "16211","16212"        // line 7
            ,"15540","15541"        // line 15
            ,"15494","15495"        // line 14
            ,"15491","8482"         // line 12
            ,"15489","15490"        // line 11
            ,"15487","15488"        // line 9
            ,"15485"                // line 8
            ,"15437","15444"                // line 19
            ,"15440","15441","15442","15443"    // line 18
            ,"15438","15439"        // line 10
            ,"8477","8480"          // line 11א
            ,"6660","6661", "6656"          // line 417
            ,"15523","15524"  // line 410
            ,"15525","15526"  // line 411
            ,"15544","15545"  // line 412
            ,"15527","15528"  // line 415
            ,"15552","15553"  // line 414
            ,"15529","15530"  // line 416
            ,"15531","15532"  // line 420
            ,"16066","16067"  // line 419

/*
*/

    );

    @Test
    public void testBeitShemeshRoutes() {
        String aDate = "2019-05-26";

        String result1 = measure(() -> processAll(aDate, routeIdsBS));
        String result2 = measure(() -> processAll(aDate, routeIdsBS));    // second time is supposed to be much faster...

        Assertions.assertThat(result1).isEqualTo(result2);
    }

    private String padNumber(int num, int digits) {
        String s = Integer.toString(num);
        int len = s.length();
        if (len < digits) {
            for (int i = len ; i < digits; i++) {
                s = "0" + s ;
            }
        }
        return s ;
    }

    @Test
    public void testSeveralRoutesAndDates() {
        List<String> dates = IntStream.rangeClosed(1,5).mapToObj(i -> "2019-07-" + padNumber(i,2)).collect(Collectors.toList());
        dates.forEach(date -> testAllBSRoutesAtDate(date));
    }

    @Test
    public void testCompareGtfsSiriForSeveralRoiutesAndDates() {
        List<String> dates = IntStream.rangeClosed(1,5).mapToObj(i -> "2019-07-" + padNumber(i,2)).collect(Collectors.toList());
        dates.forEach(date -> compareGtfsSiriForAllBSRoutesAtDate(date));
    }

    private String measure(java.util.function.Supplier<String> f) {
        long start = System.currentTimeMillis();
        String output = f.get();
        long end = System.currentTimeMillis();
        long duration = (end - start);
        logger.info("\n\n============================\n\n");
        logger.info("duration= {} MilliSeconds", duration);
        logger.info("\n\n============================\n\n");
        return output;
    }

    public void compareGtfsSiriForAllBSRoutesAtDate(String date) {
        List<String> routeIds = routeIdsBS;

        logger.info("start processing for date {} for {} routes...", date, routeIds.size());
        measure(() -> processShortTripForAll(date, routeIds));
    }


    public void testAllBSRoutesAtDate(String date) {
        List<String> routeIds = routeIdsBS;

        logger.info("start processing for date {} for {} routes...", date, routeIds.size());
        measure(() -> processAll(date, routeIds));
    }

    public void testAllRoutesAtDate(String date) {
        List<String> routeIds = routes
                .allRoutesByDate(date)
                .stream()
                .limit(10)
                .map(routeData -> routeData.routeId)
                .collect(Collectors.toList());

        logger.info("start processing for date {} for {} routes...", date, routeIds.size());
        measure(() -> processAll(date, routeIds));
    }

    private String processAll(String date, List<String> routeIds) {
        List<Tuple3> results = routeIds
                .stream()
                .map(routeId -> Tuple.of(routeId,
                        gtfsController.retrieveSiriAndGtfsDataForRouteAndDateAsJson(routeId, date),
                        gtfsController.retrieveShapeOfRouteAsJson(routeId, date)))
                .collect(Collectors.toList());
        //results.stream().forEach(tuple -> logger.info("date {}, \nroute {}: {}, \nshape {}: {}", date, tuple._1, tuple._2, tuple._1, tuple._3));
        String output = results.stream().map(tuple -> String.format("date {}, \nroute {}: {}, \nshape {}: {}",date, tuple._1, tuple._2, tuple._1, tuple._3)).collect(Collectors.joining("\n"));
        return output;
    }


    @Test
    public void compareTripIdToDateReads() {
        String date = "2019-07-18";
        List<String> routeIds = routeIdsBS;
        measure(() -> {
            java.util.Map<String, java.util.List<TripData>> allFromTripIdToDate = siriData.buildTripsForAllRoutesFromTripIdToDate(routeIds, date);
            // here compute json only for first item in the list
            String json = siriData.convertToJson(allFromTripIdToDate.get(allFromTripIdToDate.keySet().iterator().next()));
            return json;
        });
    }

    private String processShortTripForAll(String date, List<String> routeIds) {
        java.util.Map<String, java.util.List< TripData >> allFromTripIdToDate = siriData.buildTripsForAllRoutesFromTripIdToDate(routeIds, date);
        List<Tuple3> results = routeIds
                .stream()
                .map(routeId -> {
                    try {
                        logger.info("route= {}", routeId);
                        String json = siriData.convertToJson(allFromTripIdToDate.get(routeId));
                        Tuple3<String, String, String> result = Tuple.of(routeId,
                                json,   // instead of gtfsController.retrieveTripsOfRouteFromGtfsTripIdToDate(routeId, date),
                                gtfsController.retrieveAllTripsFromSiri(routeId, date));
                        logger.info("{\"route\": {}, \"date\": \"{}\", \"gtfs\":{} , \"siri\":{}}", routeId, date,result._2, result._3);
                        return result;
                    } catch (JsonProcessingException e) {
                        return Tuple.of(routeId, "error", "error");
                    }
                })
                .collect(Collectors.toList());
        //results.stream().forEach(tuple -> logger.info("date {}, \nroute {}: {}, \nshape {}: {}", date, tuple._1, tuple._2, tuple._1, tuple._3));
        String output = results.stream().map(tuple -> String.format("date {}, \nroute {}, gtfs: {}, \nsiri: {}",date, tuple._1, tuple._2, tuple._3)).collect(Collectors.joining("\n"));
        //logger.info(output);
        return output;
    }

}
