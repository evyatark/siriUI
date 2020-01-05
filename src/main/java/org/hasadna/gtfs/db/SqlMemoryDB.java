package org.hasadna.gtfs.db;

import org.dizitart.no2.Document;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteCollection;
import org.hasadna.gtfs.entity.KeyValue;
import org.hasadna.gtfs.repository.KeyValueRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Iterator;

import static org.dizitart.no2.filters.Filters.eq;

@Component
public class SqlMemoryDB implements MemoryDB {

    private static Logger logger = LoggerFactory.getLogger(SqlMemoryDB.class);

    @Autowired
    KeyValueRepository keyValueRepository;

    @Value("${sql.memory.db.enable:true}")
    public boolean dbEnabled;



    @PostConstruct
    public void init() {
        logger.info("init mySql memory DB");
        // connect to MySQL - already connected?
    }

    @Override
    public void writeKeyValue(String key, String value) {

        String kind = "siri";
        if (key.startsWith("shape")) {
            kind = "shape";
        }
        else if (key.startsWith("gtfs")) {
            kind = "gtfs";
        }
        else if (!"siri".equals(kind)){
            logger.warn("key starts with unknown prefix {}", key);
        }
        keyValueRepository.save(KeyValue.make(kind, key, value));
    }

    @Override
    public void displayStats() {
        logger.debug("DB stats: ");

    }

    @Override
    public void deleteSiriKey(String key) {
        keyValueRepository.deleteById(key);
    }

    @Override
    public void deleteShapeKey(String key) {
        keyValueRepository.deleteById(key);
    }

    @Override
    public String readKey(String key) {
        KeyValue kv = keyValueRepository.findByKeyed(key);
        if (kv == null) return null;
        return kv.getValue();
    }



}
