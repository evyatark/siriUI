package org.hasadna.gtfs.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import org.assertj.core.api.Assertions;
import org.hasadna.gtfs.db.MemoryDB;
import org.hasadna.gtfs.entity.StopsTimeData;
import org.hasadna.gtfs.service.Routes;
import org.hasadna.gtfs.service.SiriData;
import org.hasadna.gtfs.service.TripData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import java.util.stream.Collectors;
import java.util.stream.IntStream;


@RunWith(SpringRunner.class)
@SpringBootTest(properties = "classpath:application.properties")
public class GtfsControllerTest {

    private static Logger logger = LoggerFactory.getLogger(GtfsControllerTest.class);

    @Autowired
    Routes routes;

    @Autowired
    GtfsController gtfsController;

    @Autowired
    SiriData siriData;

    @Autowired
    MemoryDB memoryDB;

    List<String> routeIdsBS = List.of(
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
            ,"15523","15524"  // line 410
            ,"15525","15526"  // line 411
            ,"15544","15545"  // line 412
            ,"15527","15528"  // line 415
            ,"15552","15553"  // line 414
            ,"15529","15530"  // line 416
            ,"16066","16067"  // line 419
            ,
            "15531","15532"  // line 420
            ,"6660","6661", "6656"          // line 417

/*
*/

    );

    RestTemplate restTemplate = new RestTemplate();

    @Test
    public void testListOfRoutes1() {
        String routes = routeIdsBS.collect(Collectors.joining(","));
        logger.debug(routes);
        Assertions.assertThat(routes).contains("15438");
        List datesWithProblems = List.ofAll(
            Stream.rangeClosed(1, 30)
                .toJavaParallelStream()
                .map(i -> Integer.toString(i))
                .map(s -> (s.length() == 1)? ("0" + s) : s)
                .map(s -> "2019-11-" + s)
                .map(date -> {
                    String url = "localhost:8080/siriForDateAndRoutes/" + date + "?routeIds=" + routes;
                    String result = restTemplate.getForObject(url, String.class);
                    if (!"OK".equals(result)) {
                        logger.warn("errors in {}: {}", url, result);
                        return date + " " + result;
                    }
                    return result;
                })
                .filter(result -> !result.equals("OK"))
                .collect(Collectors.toList())
        );
        datesWithProblems.forEach(problem -> logger.error("error in {}", problem));
    }

    @Test
    public void test1() {
        /*
        @GetMapping("gtfs/shape/{routeId}/{date}")
    public String retrieveShapeOfRouteAsJson(@PathVariable String routeId, @PathVariable String date) {
        logger.info("===> gtfs/shape/{}/{}",routeId,date);
        json for shape 106264: {"shapeId": "106264", "shape": [[31
         */
        /*
        if you do:
        gtfsController.deletePreviousEntry = true;
        then the operation will reread the shape from GTFS
        but then do gtfsController.deletePreviousEntry = false ; in the end of test
        */
        String json = gtfsController.retrieveShapeOfRouteAsJson("15532", "2019-10-10");
        Assertions.assertThat(json).isNotNull().isNotEmpty();
        Assertions.assertThat(json).startsWith("{\"shapeId\": \"106264\", \"shape\": [[31.789159,35.202621],[31.789143,35.203053],");

        // if key in db - 50 ms
        // if not - 15 seconds
        String date = "2019-10-10";
        for (String rid : routeIdsBS) {
            gtfsController.retrieveShapeOfRouteAsJson(rid, date);
        }
    }

    @Test
    public void test2() {
        /*
        @GetMapping("gtfs/lines/{date}")
    public String retrieveAllLinesByDate(@PathVariable String date) {
        logger.info("===> gtfs/lines/{}",date);
         */
        //gtfsController.deletePreviousEntry = true;
        // routes.allRoutesAsJson is cached into "routeIdsByDate"
        // but cache is cleared when process restarts
        // without cache (first time) - 200 ms
        String json = gtfsController.retrieveAllLinesByDate("2019-10-10");
        // with cache - 1 ms
        json = gtfsController.retrieveAllLinesByDate("2019-10-10");
        Assertions.assertThat(json).isNotNull().isNotEmpty();
        Assertions.assertThat(json).startsWith("[{\"routeId\":\"1\",\"agencyCode\":\"25\",\"shortName\":\"1\",\"from\"");

        IntStream.range(11,32)
                .mapToObj(i -> Integer.toString(i))
                .map(day -> "2019-10-" + day)
                .forEach( date -> gtfsController.retrieveAllLinesByDate(date));
    }
    /*
    gtfs/shape/15532/2019-10-27
    json for shape 106264:

    gtfs/lines/2019-10-27
    return json (size=927235 characters)

    ===> siri/day/15532/2019-10-27
    <=== siri/day/15532/2019-10-27 return json of 476 characters
     */

    @Test
    public void test3() {
        /*
        siri/day/15532/2019-10-27
        @GetMapping("siri/day/{routeId}/{date}")
    public String retrieveSiriAndGtfsDataForRouteAndDateAsJson(@PathVariable String routeId, @PathVariable String date)
         */
        //gtfsController.deletePreviousEntry = true;
        String json = gtfsController.retrieveSiriAndGtfsDataForRouteAndDateAsJson("15532","2019-10-09");
        Assertions.assertThat(json).isNotNull().isNotEmpty();

        String date = "2019-10-10";
        for (String rid : routeIdsBS) {
            gtfsController.retrieveSiriAndGtfsDataForRouteAndDateAsJson(rid, date);
        }
    }

    @Test
    public void test4() {
        /*
        gtfs/stops/15532/41268220/2019-10-10
        @GetMapping("gtfs/stops/{routeId}/{tripId}/{date}")
    public java.util.Map<String, java.util.Map<Integer, StopsTimeData>> generateStopsMap2(@PathVariable final String routeId, @PathVariable final String tripId, @PathVariable final String date ) {
         */
        java.util.Map<String, java.util.Map<Integer, StopsTimeData>> json = gtfsController.generateStopsMap2("15532", "41268220","2019-10-10");
        //Assertions.assertThat(json).isNotNull().isNotEmpty();
        json = gtfsController.generateStopsMap2("15531", "41268220","2019-10-10");
    }

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
        List<String> dates = Stream.rangeClosed(1,5).map(i -> "2019-07-" + padNumber(i,2)).toList();
        dates.forEach(date -> testAllBSRoutesAtDate(date));
    }

    @Test
    public void testCompareGtfsSiriForSeveralRoiutesAndDates() {
        List<String> dates = Stream.rangeClosed(1,5).map(i -> "2019-07-" + padNumber(i,2)).toList();
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
                .take(10)
                .map(routeData -> routeData.routeId);

        logger.info("start processing for date {} for {} routes...", date, routeIds.size());
        measure(() -> processAll(date, routeIds));
    }

    private String processAll(String date, List<String> routeIds) {
        List<Tuple3> results = routeIds
                .map(routeId -> Tuple.of(routeId,
                        gtfsController.retrieveSiriAndGtfsDataForRouteAndDateAsJson(routeId, date),
                        gtfsController.retrieveShapeOfRouteAsJson(routeId, date)));
        //results.stream().forEach(tuple -> logger.info("date {}, \nroute {}: {}, \nshape {}: {}", date, tuple._1, tuple._2, tuple._1, tuple._3));
        String output = results.map(tuple -> String.format("date {}, \nroute {}: {}, \nshape {}: {}",date, tuple._1, tuple._2, tuple._1, tuple._3)).collect(Collectors.joining("\n"));
        return output;
    }


    @Test
    public void compareTripIdToDateReads() {
        String date = "2019-07-18";
        List<String> routeIds = routeIdsBS;
        measure(() -> {
            Map<String, List<TripData>> allFromTripIdToDate = siriData.buildTripsForAllRoutesFromTripIdToDate(routeIds, date);
            // here compute json only for first item in the list
            String json = siriData.convertToJson(allFromTripIdToDate.get(allFromTripIdToDate.keySet().iterator().next()).get());
            return json;
        });
    }

    private String processShortTripForAll(String date, List<String> routeIds) {
        Map<String, List< TripData >> allFromTripIdToDate = siriData.buildTripsForAllRoutesFromTripIdToDate(routeIds, date);
        List<Tuple3> results = routeIds
                .map(routeId -> {
                    try {
                        logger.info("route= {}", routeId);
                        String json = siriData.convertToJson(allFromTripIdToDate.get(routeId).get());
                        Tuple3<String, String, String> result = Tuple.of(routeId,
                                json,   // instead of gtfsController.retrieveTripsOfRouteFromGtfsTripIdToDate(routeId, date),
                                gtfsController.retrieveAllTripsFromSiri(routeId, date));
                        logger.info("{\"route\": {}, \"date\": \"{}\", \"gtfs\":{} , \"siri\":{}}", routeId, date,result._2, result._3);
                        return result;
                    } catch (JsonProcessingException e) {
                        return Tuple.of(routeId, "error", "error");
                    }
                });
        //results.stream().forEach(tuple -> logger.info("date {}, \nroute {}: {}, \nshape {}: {}", date, tuple._1, tuple._2, tuple._1, tuple._3));
        String output = results
                .map(tuple -> String.format("date {}, \nroute {}, gtfs: {}, \nsiri: {}",date, tuple._1, tuple._2, tuple._3))
                .collect(Collectors.joining("\n"));
        //logger.info(output);
        return output;
    }


    @Test
    public void displayMemoryDB() {
        memoryDB.displayStats();
    }

}
