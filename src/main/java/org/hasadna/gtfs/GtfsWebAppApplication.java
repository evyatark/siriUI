package org.hasadna.gtfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
//@EnableCaching
public class GtfsWebAppApplication {

    public static void main(String[] args) {
        SpringApplication.run(GtfsWebAppApplication.class, args);
    }

}
