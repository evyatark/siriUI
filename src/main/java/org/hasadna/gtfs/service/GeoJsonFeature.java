package org.hasadna.gtfs.service;

public abstract class GeoJsonFeature {
    public String type = "Feature";
    public PointGeometry geometry;

    // sub-classes will typically add a field similar to this one:
    //public SiriProperties properties;
}

