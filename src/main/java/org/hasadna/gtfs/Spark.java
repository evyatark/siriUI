package org.hasadna.gtfs;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import org.hasadna.gtfs.service.Routes;
import org.hasadna.gtfs.service.SiriData;
import org.hasadna.gtfs.service.TripData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class Spark {

    @Autowired
    SiriData siriData;

    public java.util.Map<String, String> dayResultsOfAllRoutes = new ConcurrentHashMap<>();


    public void populateDayResults(String date) {

        dayResultsOfAllRoutes = generateJsonForAllRoutes(allRouteIdsFromGtfsByDate(date), date);
    }

    public String generateJsonForRouteAndDate(String routeId, String date) {
        return siriData.dayResults(routeId, date, true);
    }

    public java.util.Map<String, String> generateJsonForAllRoutes(List<String> routeIds, String date) {
        java.util.Map<String, String> map = new HashMap<>();
        for (String routeId : routeIds) {
            map.put(routeId, generateJsonForRouteAndDate(routeId, date));
        }
        return map;
    }

    private void onlySiriTripsData(String date) {
        // from GTFS of date:
        List<String> routeIds = allRouteIdsFromGtfsByDate(date);

        // from all Siri files of that date:
        java.util.Map<String, java.util.List<TripData>> allSiriData = allSiriDataByDate(date, routeIds);
    }

    public List<String> allRouteIdsFromGtfsByDate(String date) {
        Routes r = new Routes();
        // from GTFS of date:
        List<Routes.RouteData> routes = r.allRoutesByDate(date);
        List<String> routeIds = List.ofAll(routes).map(routeData -> routeData.routeId);
        return routeIds;
    }


    public java.util.Map<String, java.util.List<TripData>> allSiriDataByDate(String date, List<String> routeIds) {
        Map<String, java.util.List<TripData>> allSiriData = null;//new HashMap<String, java.util.List<TripData>>();
//        for (String routeId : routeIds) {
//            Map<String, Stream<String>> trips = findTrips(routeId, date); // key routeId, value list of tripIds
//            java.util.List<TripData> tripsData = buildTripData(trips, date, routeId);
//            allSiriData.put(routeId, tripsData);
//        }
//        routeIds.map(routeId -> {
//            Map<String, Stream<String>> trips = findTrips(routeId, date); // key routeId, value list of tripIds
//            java.util.List<TripData> tripsData = buildTripData(trips, date, routeId);
//            return tripsData;
//        });
        allSiriData =
            io.vavr.collection.HashMap.ofEntries(
                routeIds.map(routeId ->
                        Tuple.of(routeId,
                                buildTripData(findTrips(routeId, date), date, routeId).toJavaList()
                        )
                )
            );
        return allSiriData.toJavaMap();
    }

    private List<TripData> buildTripData(Map<String, Stream<String>> trips, String date, String routeId) {
        return null;
    }

    private Map<String, Stream<String>> findTrips(String routeId, String date) {
        return null;
    }

}
