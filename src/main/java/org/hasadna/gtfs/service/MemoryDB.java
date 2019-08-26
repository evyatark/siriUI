package org.hasadna.gtfs.service;

import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;
import com.oath.halodb.HaloDBStats;
import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;
import org.dizitart.no2.tool.ExportOptions;
import org.dizitart.no2.tool.Exporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.util.Iterator;

import static org.dizitart.no2.filters.Filters.eq;

@Component
public class MemoryDB {

    private static Logger logger = LoggerFactory.getLogger(MemoryDB.class);

    @Value("${halo.db.dir}")
    public String directoryOfHaloDB;

    @Value("${halo.db.enable:false}")
    public boolean enabled;

    HaloDB db = null;
    Nitrite ndb = null;

    @PostConstruct
    public void init() {
        if (!enabled) return;
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

        // nitrite db
        ndb = Nitrite.builder()
                .filePath("/tmp/nitrite.db")
                .openOrCreate();
//        NitriteCollection collection1 = ndb.getCollection("shapes");
//        NitriteCollection collection2 = ndb.getCollection("siri");
//
//        Exporter exporter = Exporter.of(ndb);
//        exporter.exportTo("/tmp/export.txt");

    }

    public void writeKeyValue(String key, String value) {
        if (!enabled) return;

        if (db == null) return;
        logger.debug("writing key {} to memory DB", key);
        try {
            byte[] key1 = key.getBytes(Charset.forName("UTF-8"));
            byte[] value1 = value.getBytes(Charset.forName("UTF-8"));
            db.put(key1, value1);
            displayStats();
        } catch (Exception e) {
            logger.error("unhandled exception when trying to write key " + key, e);
        }

        if (key.startsWith("shape")) {
            ndb.getCollection("shapes").insert(Document.createDocument("key", key).put("value", value));
        }
        else {
            ndb.getCollection("siri").insert(Document.createDocument("key", key).put("value", value));
        }
        // after each write export to txt file - temporary!!!
        ExportOptions exportOpt = new ExportOptions();
        exportOpt.setExportData(true);
        exportOpt.setExportIndices(false);
        Exporter exporter = Exporter.of(ndb).withOptions(exportOpt);
        exporter.exportTo("/tmp/export.txt");


    }

    public void displayStats() {
        if (!enabled) return;

        HaloDBStats stats = db.stats();
        logger.debug("halo DB stats: {}", stats.toString());
    }

    public String readKey(String key) {
        if (!enabled) return null;

        if (db == null) return null;

        // Nitrite
        try {
            if (key.startsWith("shape")) {
                NitriteCollection c = ndb.getCollection("shapes");
                Iterator<Document> it = c.find(eq("key", key)).iterator();
                if (it.hasNext()) {
                    logger.warn("found key {} in Nitrite shapes", key);
                    return it.next().get("value", String.class);
                }
            }
            else {
                NitriteCollection c = ndb.getCollection("siri");
                Iterator<Document> it = c.find(eq("key", key)).iterator();
                if (it.hasNext()) {
                    logger.warn("found key {} in Nitrite siri", key);
                    return it.next().get("value", String.class);
                }
            }
            // reached here --> Nitrite does not have this key. coninue to HaloDB
        }
        catch (Exception ex) {

        }
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
