package org.hasadna.gtfs.service;

public class StopFeatureCollection {

    public String type = "FeatureCollection";
    public StopFeature[] features;

    public StopFeatureCollection(StopFeature[] features) {
        this.features = features;
    }
}
