package org.hasadna.gtfs.service;

import org.assertj.core.api.Assertions;
import org.hasadna.gtfs.entity.StopData;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Map;

public class StopsTest {

    private static Logger logger = LoggerFactory.getLogger(ReadZipFileTest.class);

    @Autowired
    Stops stops;

    @Test
    public void test1() throws IOException {
        //Stops stops = new Stops("/home/evyatar/logs/work/2019-04/gtfs/" + "gtfs2019-04-01" + ".zip") ;
        String gtfsZipFileName = "gtfs2019-04-01" + ".zip" ;
        stops.gtfsZipFileDirFullPath = "/home/evyatar/logs/work/2019-04/gtfs/";
        Map<String, StopData> stopsMap = stops.readStopDataFromFile(gtfsZipFileName);

        String stopId = "36300";
        StopData sd = stopsMap.get(stopId);
        logger.info(sd.toString());
        Assertions.assertThat(sd.stop_code).isEqualTo("21256");

        Assertions.assertThat(stopsMap.get("36300").stop_code).isEqualTo("21256");
    }
}
