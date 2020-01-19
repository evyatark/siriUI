package org.hasadna.gtfs.service;

import com.google.transit.realtime.My;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public class GtfsRtReaderTest {

    private static Logger logger = LoggerFactory.getLogger(GtfsRtReaderTest.class);

    @Test
    public void testReadGtfsRt() throws IOException {
        String fileName = "moovit.sample.message";
        InputStream f = new ClassPathResource(fileName).getInputStream();
        My.FeedMessage feedMessage = com.google.transit.realtime.My.FeedMessage.parseFrom(f);
        logger.trace(feedMessage.toString());
        int counter = 0 ;
        Map<String, Integer> countTripUpdates = new HashMap<>();
        for (My.FeedEntity entity : feedMessage.getEntityList()) {
            logger.trace("entity {}, id={}, {}", counter++, entity.getId(), //entity.getClass().getName());
                    (entity.hasAlert()?"hasAlert":"") +
                            //(entity.hasId()?"hasId":"") +
                            (entity.hasTripUpdate()?"TripUpdate ":"") +
                            (entity.hasVehicle()?"Vehicle ":"") +
                            (entity.hasIsDeleted()?"hasIsDeleted":"") );
            if (entity.hasTripUpdate() && true) {
                My.TripUpdate e1 = entity.getTripUpdate();
                int currentCount = countTripUpdates.getOrDefault(e1.getTrip().getTripId(), 0);
                countTripUpdates.put(e1.getTrip().getTripId(),currentCount+1);
                        logger.info("tripUpdate, trip={}, vehicle={}, {} StopTime Updates",
                            e1.getTrip(),
                            e1.getVehicle(),
                            e1.getStopTimeUpdateCount(),
                            e1.getStopTimeUpdateList());
                for (My.TripUpdate.StopTimeUpdate stu : e1.getStopTimeUpdateList()) {
                    logger.info("stop details id={} sequence={} arrival{}",
                        stu.getStopId(), stu.getStopSequence(), toD(stu.getArrival().getTime()));
                }
            }
            else if (entity.hasVehicle() && true) {
                My.VehiclePosition e2 = entity.getVehicle();
                logger.info("Vehicle, timestamp={} status={} stopSequence={}, position={}, vid={}, tripId={}",
                        //e2.getTimestamp() + " " +
                        LocalDateTime.ofEpochSecond(e2.getTimestamp(), 0, ZoneOffset.ofHours(2)),
                        e2.getCurrentStatus(),
                        e2.getCurrentStopSequence(),
                        e2.getPosition().getLatitude() + "," + e2.getPosition().getLongitude(),
                        e2.getVehicle().getId(),
                        e2.getTrip().getTripId()
                );
            }
        }

        // count tripIds
        for (String tripId : countTripUpdates.keySet()) {
            logger.info("trip {} count {}", tripId, countTripUpdates.get(tripId));
        }
    }

    private static LocalDateTime toD(long timestamp) {
        return LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.ofHours(2));
    }
}
