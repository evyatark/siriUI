package org.hasadna.gtfs.service;

import org.hasadna.gtfs.controller.GtfsController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

@Component
public class Meanwhile {

    private static Logger logger = LoggerFactory.getLogger(Meanwhile.class);

    @Autowired
    Routes routes;

    @Autowired
    SiriData siriData;

    //@Scheduled(initialDelay = 15000)
    @PostConstruct
    public void Compute() {
        try {
            Thread.sleep(15 * 1000);
        } catch (InterruptedException e) { }
        String date="2019-05-24";
        for (Routes.RouteData route : routes.allRoutesByDate(date)) {
            logger.info("{}...", route.routeId);
            String json = siriData.dayResults(route.routeId, date);
            logger.info("{},{} ==> {}", route.routeId, date, json);
        }
        logger.warn("=========== Done for {}  ==========", date);
    }
}
