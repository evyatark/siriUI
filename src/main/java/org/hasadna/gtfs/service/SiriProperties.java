package org.hasadna.gtfs.service;

public class SiriProperties {
    public String time_recorded;
    public String timestamp;
    public String recalculatedETA;
    public long distanceFromStart;    // in meters. This is computed by Linear Referencing the shape, with all geographic coordinates converted to UTM!!
}
