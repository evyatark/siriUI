package org.hasadna.gtfs.service;

import org.hasadna.gtfs.controller.GtfsController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
public class Meanwhile {

    private static Logger logger = LoggerFactory.getLogger(Meanwhile.class);

    private final int NUMBER_OF_THREADS = 5;

    @Autowired
    Routes routes;

    @Autowired
    SiriData siriData;

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

    //@PostConstruct
    public void parallelCompute() throws InterruptedException {
        String date="2019-05-24";
        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

        List<Callable<String>> taskList = new ArrayList<>();

        for (Routes.RouteData route : routes.allRoutesByDate(date)) {
            taskList.add(
                    () -> siriData.dayResults(route.routeId, date)
            );
        }

        logger.info("task list has {} tasks (each task processes one route", taskList.size());

        List<Future<String>> futures = executorService.invokeAll(taskList);

        List<String> results =
            futures.stream().map(future -> {
                try {
                    while (!future.isDone()) {
                        // wait
                        Thread.sleep(200);
                    }
                    String json = future.get();
                    logger.info("completed task {}", json.substring(0, 200));
                    return json;
                }
                catch (Exception e) {
                    logger.error("Exception for task", e);
                    return "";
                }
            })
            .collect(Collectors.toList());

        logger.info("completed processing, has {} items", results.size());

        writeToFile(results);
    }



    public void writeToFile(List<String> results) {
        String msg = results.stream().reduce((s,v) -> s + "\n,\n" + v).orElse("");
        try {
            String x = "" + System.currentTimeMillis();
            Files.write(Paths.get("./results_" + x + ".json"), msg.getBytes(Charset.forName("UTF-8")));
        } catch (IOException e) {
            logger.error("failed to write to file", e);
        }
    }
}
