package org.hasadna.gtfs.service;

import com.fasterxml.jackson.core.JsonParser;
import com.jayway.jsonpath.JsonPath;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;

@Component
public class SchedulesData {

    private static Logger logger = LoggerFactory.getLogger(SchedulesData.class);

    @Value("${siriSchedulesDirectory}")
    public String siriScheduleFilesDirectory;    // /home/evyatar/logs/data/



        public List<String> findAllSchedules(final String routeId, final String date) {
            try {
                logger.info("find schedules for {} on {}", routeId, date);
                java.util.List<String> lines =
                        Files.list(Paths.get(siriScheduleFilesDirectory))
                                .filter(path -> findFileForDate(path, date))
                                .map(s -> {
                                    logger.info(s.toString());
                                    return s;
                                })
                                .filter(path -> containsRoute(routeId, path))  // there should be only one such file (or 0)
                                .flatMap(path -> {
                                    try {
                                        return Files.lines(path);
                                    } catch (IOException e) {
                                        return java.util.stream.Stream.empty();
                                    }
                                })
                                .collect(Collectors.toList());
                logger.warn("reading {} lines of schedule file", lines.size());

                // parse the JSON from these lines
                String json = lines.stream().collect(Collectors.joining("\n"));
                if (StringUtils.isEmpty(json)) return null;
                java.util.List<java.util.Map<String, Object>> dataForThisRoute = JsonPath.parse(json)
                        .read("$.data[?].weeklyDepartureTimes", List.class, filter(where("lineRef").is(routeId)));


                logger.info("found this {} for route {} on date {}", dataForThisRoute, routeId, date);

                if (dataForThisRoute.isEmpty()) return null;
                java.util.Map<String, Object> jsonEntity = dataForThisRoute.get(0);

                DayOfWeek dayOfWeek = LocalDate.parse(date).getDayOfWeek();
                List<String> listOfTimesForThatDayOfWeek = new ArrayList<>();
                if (jsonEntity.containsKey(dayOfWeek.toString())) {
                    Object obj = jsonEntity.get(dayOfWeek.toString());
                    listOfTimesForThatDayOfWeek = (List<String>) obj;
                    logger.info("found {}", listOfTimesForThatDayOfWeek);
                }
                return listOfTimesForThatDayOfWeek;
            }
            catch (Exception ex) {
                // absorbing
                logger.error("can't find schedule", ex);
                return new ArrayList<>();
            }
        }

    private boolean findFileForDate(Path path, String date) {
            //logger.info("path={}", path.toFile().getName());
            return (path.toString().endsWith(date));
    }

    private boolean containsRoute(String routeId, Path path) {
        try {
            logger.info(path.toString());
            return Files.isReadable(path) && Files.lines(path).anyMatch(line -> line.contains(  "\"lineRef\" : \"" + routeId + "\"" ));
        } catch (IOException e) {
            return false;
        }
    }
}
