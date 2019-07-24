package org.hasadna.gtfs;

import org.hasadna.gtfs.controller.GtfsController;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class GtfsWebAppApplicationTests {

    @Autowired
    GtfsController gtfsController;

    @Test
    public void contextLoads() {
        gtfsController.retrieveSiriAndGtfsDataForRouteAndDateAsJson("1", "2019-05-22");
    }

}
