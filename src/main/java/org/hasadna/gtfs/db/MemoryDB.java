package org.hasadna.gtfs.db;



public interface MemoryDB {


    public void writeKeyValue(String key, String value) ;
    public void displayStats() ;

    public void deleteSiriKey(String key);

    public void deleteShapeKey(String key);

    public String readKey(String key) ;



}
