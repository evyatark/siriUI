package org.hasadna.gtfs.controller;

import io.vavr.collection.List;
import io.vavr.collection.Stream;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import java.util.stream.Collectors;


@RunWith(SpringRunner.class)
public class LongOperationsControllerTest {

    private static Logger logger = LoggerFactory.getLogger(LongOperationsControllerTest.class);

    List<String> routeIdsBS = List.of(
             "16211","16212"        // line 7
            ,"15540","15541"        // line 15
            ,"15494","15495"        // line 14
            ,"15491","8482"         // line 12
            ,"15489","15490"        // line 11
            ,"15487","15488"        // line 9
            ,"15485"                // line 8
            ,"15437","15444"                // line 19
            ,"15440","15441","15442","15443"    // line 18
            ,"15438","15439"        // line 10
            ,"8477","8480"          // line 11א
            ,"15523","15524"  // line 410
            ,"15525","15526"  // line 411
            ,"15544","15545"  // line 412
            ,"15527","15528"  // line 415
            ,"15552","15553"  // line 414
            ,"15529","15530"  // line 416
            ,"16066","16067"  // line 419
            ,
            "15531","15532"  // line 420
            ,"6660","6661", "6656"          // line 417

/*
*/

    );

    RestTemplate restTemplate = new RestTemplate();

    @Test
    public void testListOfRoutes1() {
        String routes = routeIdsBS.collect(Collectors.joining(","));
        logger.debug(routes);
        Assertions.assertThat(routes).contains("15438");
        List datesWithProblems = List.ofAll(
            Stream.rangeClosed(20, 31)
                .map(i -> Integer.toString(i))
                .map(s -> (s.length() == 1)? ("0" + s) : s)
                .map(s -> "2019-10-" + s)
                .map(date -> {
                    String url = "http://localhost:8080/siriForDateAndRoutes/" + date + "?routeIds=" + routes;
                    String result = restTemplate.getForObject(url, String.class);
                    if (!"OK".equals(result)) {
                        logger.warn("errors in {}: {}", url, result);
                        return date + " " + result;
                    }
                    return result;
                })
                .filter(result -> !result.equals("OK"))
                .collect(Collectors.toList())
        );
        datesWithProblems.forEach(problem -> logger.error("error in {}", problem));
    }


}
