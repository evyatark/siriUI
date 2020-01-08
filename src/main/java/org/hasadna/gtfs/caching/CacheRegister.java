//package org.hasadna.gtfs.caching;
//
//import io.vavr.collection.List;
//import org.hasadna.gtfs.service.Routes;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.actuate.metrics.cache.CacheMetricsRegistrar;
//import org.springframework.cache.Cache;
//import org.springframework.cache.CacheManager;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//
//@Component
//public class CacheRegister {
//    private static Logger logger = LoggerFactory.getLogger(CacheRegister.class);
//
//    private final List<String> cacheNames = List.of("tripLinesFromGtfsFileByDate", "routeIdsByDate", "shapeLinesFromGtfsFileByDate", "shapeByRouteAndDate", "stopTimesTextLineMap");
//
//    @Autowired
//    private CacheMetricsRegistrar cacheMetricsRegistrar;
//
//    @Autowired
//    private CacheManager cacheManager;
//
//    public CacheRegister() {
////        cacheManager = _cacheManager;
////        cacheMetricsRegistrar = _cacheMetricsRegistrar;
//        register();
//    }
//
//    @PostConstruct
//    public void register() {
//        // you have just registered cache "xyz"
//        for (String cacheName : cacheNames) {
//            logger.info(" registering cache {}", cacheName);
//            Cache xyz = this.cacheManager.getCache(cacheName);
//            this.cacheMetricsRegistrar.bindCacheToRegistry(xyz);
//        }
//    }
//
//
//    public CacheMetricsRegistrar getCacheMetricsRegistrar() {
//        return cacheMetricsRegistrar;
//    }
//
//    public void setCacheMetricsRegistrar(CacheMetricsRegistrar cacheMetricsRegistrar) {
//        this.cacheMetricsRegistrar = cacheMetricsRegistrar;
//    }
//
//    public CacheManager getCacheManager() {
//        return cacheManager;
//    }
//
//    public void setCacheManager(CacheManager cacheManager) {
//        this.cacheManager = cacheManager;
//    }
//}
