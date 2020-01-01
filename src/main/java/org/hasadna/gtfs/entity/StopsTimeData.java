package org.hasadna.gtfs.entity;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import org.hasadna.gtfs.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;


public class StopsTimeData {

    private static Logger logger = LoggerFactory.getLogger(StopsTimeData.class);

    public String trip_id ;
    public String arrivalTime;
    public String departureTime;
    public String stop_id ;
    public String stop_sequence;
    public String distance;     // in meters
    public StopData stopData;

    public StopsTimeData(String trip_id, String arrivalTime, String departureTime, String stop_id, String stop_sequence, String distance ) {
        this.trip_id = trip_id;
        this.stop_id = stop_id;
        this.stop_sequence = stop_sequence;
        this.arrivalTime = arrivalTime;
        this.departureTime = departureTime;
        this.distance = distance;
    }

    // headers in stop_times.txt:
    // trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type,shape_dist_traveled
    public static StopsTimeData extractFrom(final String line) {

        String[] data = line.split(",");
        logger.debug("extract {}, {}", data[0], data[3]);
        return new StopsTimeData(
                data[0], data[1], data[2], data[3], data[4], data[7]
        );
    }


    private static StopFeature createStopPart(final StopsTimeData std) {
        StopFeature sf = new StopFeature();
        sf.geometry = new PointGeometry();
        sf.geometry.coordinates = extractGpsLatLong(std);
        sf.properties = new StopProperties();
        sf.properties.trip_id = std.trip_id;
        sf.properties.stop_sequence = std.stop_sequence;
        sf.properties.stop_id = std.stop_id;
        sf.properties.stop_code = std.stopData.stop_code;
        sf.properties.stop_name = std.stopData.stop_name;
        sf.properties.stop_desc = std.stopData.stop_desc;
        sf.properties.distance = std.distance;
        sf.properties.location_type = std.stopData.location_type;
        sf.properties.zone_id = std.stopData.zone_id;
        sf.properties.arrivalTime = std.arrivalTime;
        sf.properties.departureTime = std.departureTime;
        return sf;
    }

    public static StopFeatureCollection createFeatures(final Map<Integer, StopsTimeData> stopsTimeData) {
        if ((stopsTimeData == null) || stopsTimeData.isEmpty()) {
            logger.warn("WARNING: null or empty data about stops time!");
            return new StopFeatureCollection(new StopFeature[]{});  // empty
        }
        List<StopFeature> sfs = stopsTimeData.keySet()
                .map(stopSequenceStr ->
                        createStopPart(stopsTimeData.get(stopSequenceStr).get())).toList();
        StopFeature[] stopFeaturesArr = listToArray(sfs);
        return new StopFeatureCollection(stopFeaturesArr);
    }

    private static StopFeature[] listToArray(List<StopFeature> sfs) {
        StopFeature[] arr = new StopFeature[sfs.size()];
        for (int i = 0 ; i < sfs.size() ; i++) {
            arr[i] = sfs.get(i);
        }
        return arr;
    }


    private static String[] extractGpsLatLong(final StopsTimeData std) {
        String lat = std.stopData.stop_lat;
        String longitude = std.stopData.stop_lon;
        return new String[]{lat, longitude};
    }

    @Override
    public String toString() {
        return "StopsTimeData{" +
                "trip_id='" + trip_id + '\'' +
                ", stop_id='" + stop_id + '\'' +
                ", stop_sequence='" + stop_sequence + '\'' +
                ", arrivalTime='" + arrivalTime + '\'' +
                ", departureTime='" + departureTime + '\'' +
                '}';
    }
}
