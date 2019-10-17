package org.hasadna.gtfs.service;

import org.hasadna.gtfs.entity.StopsTimeData;

import java.util.List;
import java.util.Map;

public class TripData {

    String routeId;
    String shortName;
    String agencyCode;
    String agencyName;
    String dayOfWeek;
    String date;
    String originalAimedDeparture;
    String gtfsETA;
    String gtfsTripId;
    Boolean suspicious ;
    Boolean dns ;           // DNS = did Not Start!

    public Map<Integer, StopsTimeData> stopsTimeData;

    String siriTripId;
    String alternateTripId = "";
    String vehicleId;
    //List<SiriReading> siri1;
    SiriFeatureCollection siri;
    StopFeatureCollection stops;
    /*
    {
        route
        shortName
        agencyCode
        agencyName
        date
        originalAimedDeparture
        gtfsEta
        gtfsTripId
        siriTripId
        vehicleId
        siri1: [
            {
                timestamp
                timestamp_gps
                lat,long
                recalculated_eta
        ]

    }
    */

    public String getAlternateTripId() {
        return alternateTripId;
    }

    public void setAlternateTripId(String alternateTripId) {
        this.alternateTripId = alternateTripId;
    }

    public String getRouteId() {
        return routeId;
    }

    public void setRouteId(String routeId) {
        this.routeId = routeId;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public String getAgencyCode() {
        return agencyCode;
    }

    public void setAgencyCode(String agencyCode) {
        this.agencyCode = agencyCode;
    }

    public String getAgencyName() {
        return agencyName;
    }

    public void setAgencyName(String agencyName) {
        this.agencyName = agencyName;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public void setDayOfWeek(String dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getOriginalAimedDeparture() {
        return originalAimedDeparture;
    }

    public void setOriginalAimedDeparture(String originalAimedDeparture) {
        this.originalAimedDeparture = originalAimedDeparture;
    }

    public String getGtfsETA() {
        return gtfsETA;
    }

    public void setGtfsETA(String gtfsETA) {
        this.gtfsETA = gtfsETA;
    }

    public String getGtfsTripId() {
        return gtfsTripId;
    }

    public void setGtfsTripId(String gtfsTripId) {
        this.gtfsTripId = gtfsTripId;
    }

    public String getSiriTripId() {
        return siriTripId;
    }

    public void setSiriTripId(String siriTripId) {
        this.siriTripId = siriTripId;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }


    public SiriFeatureCollection getSiri() {
        return siri;
    }

    public void setSiri(SiriFeatureCollection siri) {
        this.siri = siri;
    }

    public Boolean getSuspicious() {
        return suspicious;
    }

    public void setSuspicious(Boolean suspicious) {
        this.suspicious = suspicious;
    }

    public StopFeatureCollection getStops() {
        return stops;
    }

    public void setStops(StopFeatureCollection stops) {
        this.stops = stops;
    }

    public Boolean getDns() {
        return dns;
    }

    public void setDns(Boolean dns) {
        this.dns = dns;
    }

    @Override
    public String toString() {
        return "TripData{" +
                "routeId='" + routeId + '\'' +
                ", shortName='" + shortName + '\'' +
                ", agencyCode='" + agencyCode + '\'' +
                ", agencyName='" + agencyName + '\'' +
                ", dayOfWeek='" + dayOfWeek + '\'' +
                ", date='" + date + '\'' +
                ", originalAimedDeparture='" + originalAimedDeparture + '\'' +
                ", gtfsETA='" + gtfsETA + '\'' +
                ", gtfsTripId='" + gtfsTripId + '\'' +
                ", stopsTimeData=" + stopsTimeData +
                ", siriTripId='" + siriTripId + '\'' +
                ", vehicleId='" + vehicleId + '\'' +
                ", suspicious='" + suspicious + '\'' +
                ", siri=[List of " + siri.features.length + " Features]" +
                '}';
    }
}

class SiriReading {
    String timestamp;   // of query
    String timestampGPS;    // of the gps point
    String[] latLong;
    String recalculatedETA;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestampGPS() {
        return timestampGPS;
    }

    public void setTimestampGPS(String timestampGPS) {
        this.timestampGPS = timestampGPS;
    }

    public String[] getLatLong() {
        return latLong;
    }

    public void setLatLong(String[] latLong) {
        this.latLong = latLong;
    }

    public String getRecalculatedETA() {
        return recalculatedETA;
    }

    public void setRecalculatedETA(String recalculatedETA) {
        this.recalculatedETA = recalculatedETA;
    }


}
