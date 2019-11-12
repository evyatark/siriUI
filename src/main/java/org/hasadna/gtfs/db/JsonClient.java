package org.hasadna.gtfs.db;


import com.google.gson.Gson;
import com.redislabs.modules.rejson.JReJSON;
import com.redislabs.modules.rejson.Path;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Component
public class JsonClient {

    JReJSON client;
    //private Gson g = new Gson();

    @PostConstruct
    public void init() {
        // First get a connection
        client = new JReJSON("localhost", 6379);

        // Setting a Redis key name _foo_ to the string _"bar"_, and reading it back
//        client.set("foo1", "bar1");
//        String s0 = (String) client.get("foo1");
//        String s2 = (String) client.get("foo1", Path.ROOT_PATH); // same as above
//        System.out.println( s2 );
//
//        Path p = new Path(".siriLines");
//        client.set("siriLines.key1", "value1");
//        System.out.println( client.get("siriLines.key1") );
//        System.out.println( p.toString() );
    }

    public void saveSiriResultsOfRouteAndTrip(String date, String routeId, String tripId, List<String> siriResults) {
        String key = makeKey(date,routeId, tripId);
        //String json = g.toJson(siriResults);
        client.set(key, siriResults);
    }

    public List<String> getSiriResultsOfRouteAndTrip(String date, String routeId, String tripId) {
        Object obj = client.get(makeKey(date, routeId, tripId));
        List<String> result = (List<String>) obj;
        return result;
    }


    public void saveSiriResultsOfRoute(String date, String routeId, Map<String, List<String>> siriResults) {
        String key = makeKey2(date, routeId);
        //String json = g.toJson(siriResults);
        client.set(key, siriResults);
    }

    public Map<String, List<String>> getSiriResultsOfRoute(String date, String routeId) {
        String key = makeKey2(date, routeId);
        Object obj = client.get(key);
        Map<String, List<String>> result = (Map<String, List<String>>) obj;
        return result;
    }

    private String makeKey2(String date, String routeId) {
        String key = "" + date + "_" + "siri_" + routeId ;
        return key;
    }

    private String makeKey(String date, String routeId, String tripId) {
        String key = "" + date + "_" + "siri_" + routeId + "_" + tripId ;
        return key;
    }
}
