package org.hasadna.gtfs.service;

public class SiriFeatureCollection {

    public String type = "FeatureCollection";
    public SiriFeature[] features;

    public SiriFeatureCollection(SiriFeature[] features) {
        this.features = features;
    }
}
