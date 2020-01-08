//package org.hasadna.gtfs.caching;
//
//import org.ehcache.event.CacheEvent;
//import org.ehcache.event.CacheEventListener;
//import org.hasadna.gtfs.controller.GtfsController;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class CacheEventLogger implements CacheEventListener<Object, Object> {
//
//    private static Logger logger = LoggerFactory.getLogger(CacheEventLogger.class);
//
//    @Override
//    public void onEvent(CacheEvent<?, ?> cacheEvent) {
//        logger.info("Key: {} | EventType: {} | Old value: {} | New value: {}",
//                cacheEvent.getKey(), cacheEvent.getType()
////                ,cacheEvent.getOldValue(),
////                cacheEvent.getNewValue()
//                );
//    }
//}
