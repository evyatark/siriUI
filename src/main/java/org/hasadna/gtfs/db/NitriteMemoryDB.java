package org.hasadna.gtfs.db;

import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.util.Iterator;

import static org.dizitart.no2.filters.Filters.eq;

@Component
public class NitriteMemoryDB implements MemoryDB {

    private static Logger logger = LoggerFactory.getLogger(NitriteMemoryDB.class);

    @Value("${nitrite.db.enable:false}")
    public boolean nitriteEnabled;

    @Value("${tests.delete.prev.db.entry:false}")
    public boolean deletePreviousEntry;

    @Value("${nitrite.db.file.path}")
    public String nitritePath = "/home/evyatar/temp/nitrite.db";

    Nitrite ndb = null;
    boolean cancellReEvaluation = false;

    @PostConstruct
    public void init() {

        // nitrite db
        ndb = Nitrite.builder()
                .filePath(nitritePath)
                .openOrCreate();
//        NitriteCollection collection1 = ndb.getCollection("shapes");
//        NitriteCollection collection2 = ndb.getCollection("siri");
//
//        Exporter exporter = Exporter.of(ndb);
//        exporter.exportTo("/tmp/export.txt");

    }

    @Override
    public void writeKeyValue(String key, String value) {

        if (key.startsWith("shape")) {
            ndb.getCollection("shapes").insert(Document.createDocument("key", key).put("value", value));
        }
        else {
            ndb.getCollection("siri").insert(Document.createDocument("key", key).put("value", value));
        }
        // after each write export to txt file - temporary!!!
//        ExportOptions exportOpt = new ExportOptions();
//        exportOpt.setExportData(true);
//        exportOpt.setExportIndices(false);
//        Exporter exporter = Exporter.of(ndb).withOptions(exportOpt);
//        exporter.exportTo("/tmp/export.txt");


    }

    @Override
    public void displayStats() {
        logger.debug("ndb DB stats: ");
        showCollections();
    }

    @Override
    public void deleteSiriKey(String key) {
        NitriteCollection c = ndb.getCollection("siri");
        Iterator<Document> it = c.find(eq("key", key)).iterator();
        if (it.hasNext()) {
            logger.warn("found key {} in Nitrite siri", key);
            Document doc = it.next();
            c.remove(doc);
            logger.warn("removed key {} in Nitrite siri", key);
        }
    }

    @Override
    public void deleteShapeKey(String key) {
        NitriteCollection c = ndb.getCollection("shapes");
        Iterator<Document> it = c.find(eq("key", key)).iterator();
        if (it.hasNext()) {
            logger.warn("found key {} in Nitrite shapes", key);
            Document doc = it.next();
            c.remove(doc);
            logger.warn("removed key {} in Nitrite shapes", key);
        }
    }

    @Override
    public String readKey(String key) {
        if (!nitriteEnabled) return null;

        // Nitrite
        try {
            if (key.startsWith("shape")) {
                NitriteCollection c = ndb.getCollection("shapes");
                Iterator<Document> it = c.find(eq("key", key)).iterator();
                if (it.hasNext()) {
                    logger.warn("found key {} in Nitrite shapes", key);
                    Document doc = it.next();

                    return doc.get("value", String.class);
                }
            }
            else {
                NitriteCollection c = ndb.getCollection("siri");
                Iterator<Document> it = c.find(eq("key", key)).iterator();

                if (key.equals("siri$15531@2019-11-03") && !cancellReEvaluation) {
                    logger.info("key {} - value will be recalculated!", key);
                    cancellReEvaluation = true;
                    return null;
                }


                if (it.hasNext()) {
                    logger.warn("found key {} in Nitrite siri", key);
                    return it.next().get("value", String.class);
                }
            }
            // reached here --> Nitrite does not have this key. continue to HaloDB
            return null;
        }
        catch (Exception ex) {
            logger.error("unhandled exception when trying to read key " + key, ex);
            return null;
        }
    }

    public void showCollections() {
        final int LIMIT = 2;
        logger.info("Nitrite:");
        logger.info("Collections: {}", ndb.listCollectionNames());
        logger.info("Repositories: {}", ndb.listRepositories());
        for (String name : ndb.listCollectionNames()) {
            logger.info("collection {} contains {} documents", name, ndb.getCollection(name).size());
            Iterator<Document> iter = ndb.getCollection(name).find().iterator();
            int counter = 0 ;
            while (iter.hasNext()) {
                counter = counter + 1;
                Document doc = iter.next();
                //logger.info("\tDocument {} last modified at {}", doc.getId(), doc.getLastModifiedTime());
                logger.info("{} : {}", doc.get("key"), doc.get("value"));
//                Set<String> keys = doc.keySet();
//                for (String key : keys) {
//                    logger.info ("\t\t{} : {}", key, doc.get(key));
//                }
                if (counter > LIMIT) {
                    break;
                }
            }
        }
    }

}
