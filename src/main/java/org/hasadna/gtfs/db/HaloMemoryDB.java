package org.hasadna.gtfs.db;

import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;
import com.oath.halodb.HaloDBStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.util.Iterator;


//@Component
public class HaloMemoryDB implements MemoryDB {

    private static Logger logger = LoggerFactory.getLogger(HaloMemoryDB.class);

    @Value("${halo.db.dir}")
    public String directoryOfHaloDB;


    @Value("${halo.db.enable:false}")
    public boolean halodbEnabled;

//    @Value("${tests.delete.prev.db.entry:false}")
//    public boolean deletePreviousEntry;

    HaloDB db = null;

    @PostConstruct
    public void init() {
        if (halodbEnabled) {
            HaloDBOptions options = new HaloDBOptions();
            options.setMaxFileSize(1024 * 1024 * 1024);
            options.setMaxTombstoneFileSize(64 * 1024 * 1024);
            options.setBuildIndexThreads(4);
            options.setFlushDataSizeBytes(10 * 1024 * 1024);
            options.setCompactionThresholdPerFile(0.7);
            options.setCompactionJobRate(50 * 1024 * 1024);
            options.setNumberOfRecords(100_000_000);
            options.setCleanUpTombstonesDuringOpen(true);
            options.setCleanUpInMemoryIndexOnClose(false);
            options.setUseMemoryPool(true);
            options.setMemoryPoolChunkSize(2 * 1024 * 1024);
            //options.setFixedKeySize(8);
            //HaloDB db = null;
            //String directory = "../db/";
            try {
                db = HaloDB.open(directoryOfHaloDB, options);
            } catch (HaloDBException e) {
                logger.error("unhandled exception when trying to open HaloDB file", e);
            }

            //TODO where to close db?
            //db.close();
        }

    }

    @Override
    public void writeKeyValue(String key, String value) {
        if ((db == null) || !halodbEnabled) return ;

        logger.debug("writing key {} to memory DB", key);
        try {
            byte[] key1 = key.getBytes(Charset.forName("UTF-8"));
            byte[] value1 = value.getBytes(Charset.forName("UTF-8"));
            db.put(key1, value1);
            displayStats();
        } catch (Exception e) {
            logger.error("unhandled exception when trying to write key " + key, e);
        }

    }

    @Override
    public void displayStats() {
        if ((db == null) || !halodbEnabled) return ;

        HaloDBStats stats = db.stats();
        logger.debug("halo DB stats: {}", stats.toString());
    }

    @Override
    public void deleteSiriKey(String key) {
        logger.warn("not implemented");
    }

    @Override
    public void deleteShapeKey(String key) {
        logger.warn("not implemented");
    }

    @Override
    public String readKey(String key) {
        if ((db == null) || !halodbEnabled) return null;

        try {
            byte[] key1 = key.getBytes(Charset.forName("UTF-8"));
            byte[] value1 = db.get(key1);
            if (value1 == null) {
                return null;
            }
            String result = new String(value1, Charset.forName("UTF-8"));
            logger.debug("read key {} completed", key);
            return result;
        } catch (HaloDBException e) {
            logger.error("unhandled exception when trying to read key " + key, e);
            return null;
        }
    }

}
