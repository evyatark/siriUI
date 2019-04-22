package org.hasadna.gtfs.entity;

import org.hasadna.gtfs.service.ReadZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopsTimeData {

    private static Logger logger = LoggerFactory.getLogger(StopsTimeData.class);

    public String trip_id ;
    public String stop_id ;
    public String stop_sequence;
    public String arrivalTime;
    public String departureTime;
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
