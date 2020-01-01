package org.hasadna.gtfs.service;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.internal.filter.ValueNode;
import io.vavr.Tuple;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import org.assertj.core.api.Assertions;
import org.hasadna.gtfs.entity.StopData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StopWatch;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hasadna.gtfs.service.Stops.decideGtfsFileName;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SiriDataTest {

    @Autowired
    SiriData siriData;

    @Autowired
    Stops stops;

    private static Logger logger = LoggerFactory.getLogger(SiriDataTest.class);
    private static AtomicInteger count = new AtomicInteger(0);

    @Test
    public void test1() {

        Stream<String> lines = siriData.readSiriGzipFile("/home/evyatar/logs/data/siri_rt_data.2019-04-04.8.log.gz");
        lines.take(100).filter(line -> line.length() > 1).forEach(line -> logger.info(line));

    }

    @Test
    public void test2() {

        Stream<String> lines = siriData.readSiriGzipFile("/home/evyatar/logs/data/siri_rt_data.2019-04-04.8.log.gz");
        int count = 0 ;
        lines.filter(line -> line.length() > 1).forEach(line -> increaseCounter());

        int x = increaseCounter();
        logger.info("count=" + x);

    }

    @Test
    public void test3() {

        Stream<String> lines = siriData.readSiriGzipFile("/home/evyatar/logs/data/siri_rt_data.2019-04-04.8.log.gz");
        siriData.groupByRoute(lines);
    }

    @Test
    public void test4() {

        Stream<String> lines = siriData.readSeveralGzipFiles(
                "/home/evyatar/logs/data/siri_rt_data.2019-04-04.8.log.gz",
                "/home/evyatar/logs/data/siri_rt_data.2019-04-04.9.log.gz",
                "/home/evyatar/logs/data/siri_rt_data.2019-04-04.10.log.gz");
        siriData.groupByRoute(lines);
    }

    @Test
    public void test5() {
        final String ROUTE_ID = "15531";
        List<String> names = List.range(0, 11).map(i -> "/home/evyatar/logs/data/siri_rt_data.2019-04-04." + i + ".log.gz");
        Stream<String> lines = siriData
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> ROUTE_ID.equals(siriData.extractRouteId(line)));
        logger.info("I got {} readings of routeId {}", lines.toList().size(), ROUTE_ID);

        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());

        vLines.groupBy(line -> siriData.extractTripId(line));
    }

    @Test
    public void test6() {
        final String ROUTE_ID = "15531";
        List<String> names = List.range(0, 11).map(i -> "/home/evyatar/logs/data/siri_rt_data.2019-04-04." + i + ".log.gz");
        Stream<String> lines = siriData
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> !line.endsWith(",0,0"))
                .filter(line -> ROUTE_ID.equals(siriData.extractRouteId(line)));
        logger.info("I got {} readings of routeId {}", lines.toList().size(), ROUTE_ID);

        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());

        vLines.groupBy(line -> siriData.extractTripId(line));
    }

    @Test
    public void test7() {
        final String ROUTE_ID = "15531";
        List<String> names = List.range(0, 11).map(i -> "/home/evyatar/logs/data/siri_rt_data.2019-04-04." + i + ".log.gz");
        Stream<String> lines = siriData
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> !line.endsWith(",0,0"))
                .filter(line -> ROUTE_ID.equals(siriData.extractRouteId(line)));

        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());

        Map<String, io.vavr.collection.Stream<String>> trips = vLines.groupBy(line -> siriData.extractTripId(line));
        Set<String> tripNames = trips.keySet();
        logger.info("route {}, date 2019-04-04, {} trips", ROUTE_ID, tripNames.length());
        logger.info("trips: {}", tripNames.mkString(", "));
        logger.info("Count of Siri GPS points for each trip:");
        tripNames
                .map(name -> trips.get(name.toString()).get())
                .map(stream -> stream.count(i->true))
                .forEach(c -> logger.info("count: {}", c));
    }

    @Test
    public void test8() {
        final String ROUTE_ID = "15531";
        List<String> names = List.range(0, 11).map(i -> "/home/evyatar/logs/data/siri_rt_data.2019-04-04." + i + ".log.gz");
        Stream<String> lines = siriData
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> !line.endsWith(",0,0"))
                .filter(line -> ROUTE_ID.equals(siriData.extractRouteId(line)));

        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());

        Map<String, io.vavr.collection.Stream<String>> trips = vLines.groupBy(line -> siriData.extractTripId(line));
        Set<String> tripNames = trips.keySet();
        logger.info("got {} trips:", tripNames.length());
        Set<Integer> counts = tripNames
                .map(name -> trips.getOrElse(name.toString(), io.vavr.collection.Stream.empty()))
                .map(stream -> {
                    int count = stream.count(i->true);
                    if (count < 28) {
                        logger.info("trip with {} lines:", count);
                        stream.forEach(line->
                            logger.info(line));
                    }
                    return count;
                });
        tripNames.zip(counts).forEach(t -> logger.info("trip {}: {} lines", t._1, t._2));

    }


    @Test
    public void test9() {
        final String ROUTE_ID = "15531";
        List<String> names = List.range(0, 11).map(i -> "/home/evyatar/logs/data/siri_rt_data.2019-04-04." + i + ".log.gz");
        Stream<String> lines = siriData
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> !line.endsWith(",0,0"))
                .filter(line -> ROUTE_ID.equals(siriData.extractRouteId(line)));

        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());

        Map<String, io.vavr.collection.Stream<String>> trips = vLines.groupBy(line -> siriData.extractTripId(line));
        logger.info("got {} trips, {} of them suspicious", trips.size(), trips.count(trip->trip._2.count(i->true)<28));
        // display all readings of trip 37203593
        trips.getOrElse("37203593", null).forEach(line -> logger.info(line));

    }

    @Test
    public void test10() {
        final String ROUTE_ID = "15531";
        List<String> names = List.range(0, 11).map(i -> "/home/evyatar/logs/data/siri_rt_data.2019-04-04." + i + ".log.gz");
        Stream<String> lines = siriData
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> !line.endsWith(",0,0"))
                .filter(line -> ROUTE_ID.equals(siriData.extractRouteId(line)));

        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());

        Map<String, io.vavr.collection.Stream<String>> trips = vLines.groupBy(line -> siriData.extractTripId(line));
        logger.info("got {} trips, {} of them suspicious", trips.size(), trips.count(trip->trip._2.count(i->true)<28));
    }



    /*
    private void dayResults(final String routeId, String date) {
        DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
        final String ROUTE_ID = routeId;
        List<String> names = List.range(0, 20).map(i -> "/home/evyatar/logs/data/siri_rt_data." + date + "." + i + ".log.gz");  // 2019-04-04
        Stream<String> lines = siriData
                .readSeveralGzipFiles(names.toJavaArray(String.class))
                .filter(line -> line.length() > 1)
                .filter(line -> !line.endsWith(",0,0"))
                .filter(line -> ROUTE_ID.equals(siriData.extractRouteId(line)));

        io.vavr.collection.Stream <String> vLines =
                lines.collect(io.vavr.collection.Stream.collector());

        Map<String, io.vavr.collection.Stream<String>> trips = vLines.groupBy(line -> siriData.extractTripId(line));
        logger.info("{} {} route {}:got {} trips, {} of them suspicious", dayOfWeek, date, routeId, trips.size(), trips.count(trip->trip._2.count(i->true)<28));

    }
    */




    @Test
    public void test11() {
        stops.gtfsZipFileDirFullPath = "/home/evyatar/sivan/may25/" ;
        //stops.gtfsZipFileName = "gtfs2019-05-25" + ".zip";
        siriData.directoryOfMakatFile = "/home/evyatar/sivan/may25/";

        List<String> dates = List.of("2019-05-25");//, "2019-04-01", "2019-04-02", "2019-04-03", "2019-04-04", "2019-04-05");
        List<String> routes = List.of("8176");//, "15532");
        dates.forEach(date ->
            routes
                    .map(routeId -> siriData.dayResults( routeId, date, true))
                    .forEach(json -> logger.info(json))
                //routeId -> dayResults( routeId, date))
        );
    }


    @Test
    public void test12() {
        java.util.List<String> dates =
                io.vavr.collection.Stream.rangeClosed(1, 31)
                .map(i -> Integer.toString(i))  //.collect(io.vavr.collection.Stream.collector());
                .map(s -> (s.length()==1? ("0" + s) : s))
                .map(s -> "2019-03-" + s)
                .asJava();
        List<String> routes = List.of("15531", "15532", "15530", "15527", "15528", "6656, 6660", "15553");
        //String date = dates.head();
        dates.forEach(date ->
                        routes.forEach(
                                routeId -> siriData.dayResults( routeId, date, true))
                //routeId -> dayResults( routeId, date))
        );
    }


    @Test
    public void test13() {
        //Stops stops = new Stops("/home/evyatar/logs/work/2019-03/gtfs/" + "gtfs2019-03-01" + ".zip") ;
        stops.gtfsZipFileDirFullPath = "/home/evyatar/logs/work/2019-03/gtfs/" ;
        //stops.gtfsZipFileName = "gtfs2019-03-01" + ".zip";
        Map<String, StopData> stopsMap = stops.readStopDataFromFile("gtfs2019-03-01" + ".zip");

        List<String> dates =
                Stream.rangeClosed(4, 4)
                //io.vavr.collection.Stream.rangeClosed(1, 10)
                        .map(i -> Integer.toString(i))  //.collect(io.vavr.collection.Stream.collector());
                        .map(s -> (s.length()==1? ("0" + s) : s))
                        .map(s -> "2019-03-" + s)
                        .toList();
        List<String> routes = List.of("15531");
        //String date = dates.head();
        List<String> jsonList =
                dates.flatMap(date -> {
                        logger.info("processing date {}", date);
                        return routes.map(
                                routeId -> siriData.dayResults(routeId, date, true));
                    }).toList();

                //routeId -> dayResults( routeId, date))
        logger.info("there are {} json Strings in my list (one for each of the following dates: {})", jsonList.size(), dates);
        Assertions.assertThat(jsonList).isNotEmpty();
        for (int i : Stream.range(0, jsonList.size()).toArray()) {
            String json = jsonList.get(i);
            try {
                logger.info("json String #{}, date={}", i, JsonPath.read(json, "$[0].date"));
                String rid = JsonPath.parse(json).read("$[0].routeId", String.class);
                String tid = JsonPath.read(json, "$[0].siriTripId").toString();
                String oad = JsonPath.read(json, "$[0].stopsTimeData['1'].departureTime").toString();
                logger.info("routeId={}, tripId={}, aimed departure={}", rid, tid, oad);
                logger.info("first stop in route: {}", JsonPath.read(json, "$[0].stopsTimeData['1'].stopData").toString());
            }
            catch (Exception ex) {
                //logger.error("", ex);
                logger.info("some exception! json={}", json);
            }
        }
    }


    private int increaseCounter() {
        return count.incrementAndGet();
    }

/*
    @Test
    public void test14() {
        java.util.Map<String, java.util.Map<Integer, StopsTimeData>> map =
            stops.generateStopsMap(List.of("36619570").toJavaSet(), "2019-03-04",
                "/home/evyatar/logs/work/2019-03/gtfs", new HashMap<>());
        // map has key tripId and value is a map of all stops in this trip
        // generateStopsMap might return an empty map, if tripId not found in GTFS.
        // It is possible, because GTFS sometimes does not contain the real tripIds, but the Sunday tripIds!
        int size = map.keySet().size();
        logger.info("map size is {}", size);
        Assertions.assertThat(size).isGreaterThanOrEqualTo(0);

    }
*/
    @Test
    public void test15() {

        String gtfsZipFileName = "gtfs2019-03-03.zip";
        String TRIP_ID = "36619570" ;
        List<String> linesOfTrip = stops.readStopTimesFile(List.of(TRIP_ID).toSet(), "2019-03-03");
        if (linesOfTrip.isEmpty()) {
            logger.info(" GTFS file {} does not contain tripId {}", gtfsZipFileName, TRIP_ID);
        }
        else {
            linesOfTrip.forEach(line -> logger.info(line));
        }
    }

    @Value("${gtfsZipFileDirectory}")
    public String directoryOfGtfsFile;

    @Test
    public void test16() {
        String date = "2019-12-31";
        String routeId = "15527";
        String gtfsZipFileName = "gtfs" + date + ".zip";
        StopWatch sw = new StopWatch();
        sw.start();
        List<String> tripIds = siriData.findAlternateTripIds(routeId, date, null, null);
        sw.stop();
        logger.info("takes {} ms", sw.getLastTaskTimeMillis());


//        for (String tripId : tripIds) {
//            logger.info(tripId);
//        };
        Assertions.assertThat(tripIds).hasSize(40);
//        for (String rid : List.of("15527", "15528", "15527", "15528", "15527", "15528", "15532")) {
//            sw.start();
//            siriData.findAlternateTripIds(rid, date);
//            sw.stop();
//            logger.info("{} takes {} ms", rid, sw.getLastTaskTimeMillis());
//        }


        // works much faster if we read tripLines once and pass it when calling
        gtfsZipFileName = decideGtfsFileName(date);
        final String gtfsZipFileFullPath = Utils.findFile(directoryOfGtfsFile, gtfsZipFileName);
        final ReadZipFile rzf = new ReadZipFile();
        final List<String> tripLines = rzf.tripLinesFromFile(gtfsZipFileFullPath).toList();
        // The file is only 42MB so it is not much to keep in memory

        final Map<String, List<String>> serviceIdToCalendarLines = CalendarReader.make(directoryOfGtfsFile).readCalendar(date);

        for (int i = 0 ; i < 10 ; i++) {
            for (String rid : List.of("15527", "15528", "15527", "15528", "15527", "15528", "15532")) {
                sw.start();
                siriData.findAlternateTripIds(rid, date, tripLines, serviceIdToCalendarLines);
                sw.stop();
                logger.info("{} takes {} ms", rid, sw.getLastTaskTimeMillis());
            }
        }

    }


}
