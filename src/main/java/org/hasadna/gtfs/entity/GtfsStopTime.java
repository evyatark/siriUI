package org.hasadna.gtfs.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.Objects;

@Entity(name="gtfs_stop_times_raw")
public class GtfsStopTime {
    // headers in stop_times.txt:
    // trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type,shape_dist_traveled
    private String tripId ;
    private String arrivalTime;
    private String departureTime;
    private String stopId ;
    private String stopSequence;
    private String pickupType;
    private String dropOffType;
    private String distance;     // in meters

    @Id
    private int id;      // auto generated
    private String date;    // added later (not when loading file stop_times.gtfs to the DB)


    public GtfsStopTime() {
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(String arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public String getDepartureTime() {
        return departureTime;
    }

    public void setDepartureTime(String departureTime) {
        this.departureTime = departureTime;
    }

    public String getStopId() {
        return stopId;
    }

    public void setStopId(String stopId) {
        this.stopId = stopId;
    }

    public String getStopSequence() {
        return stopSequence;
    }

    public void setStopSequence(String stopSequence) {
        this.stopSequence = stopSequence;
    }

    public String getPickupType() {
        return pickupType;
    }

    public void setPickupType(String pickupType) {
        this.pickupType = pickupType;
    }

    public String getDropOffType() {
        return dropOffType;
    }

    public void setDropOffType(String dropOffType) {
        this.dropOffType = dropOffType;
    }

    public String getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return tripId +
                "," + arrivalTime +
                "," + departureTime +
                "," + stopId +
                "," + stopSequence +
                "," + pickupType +
                "," + dropOffType +
                "," + distance +
                "," + id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GtfsStopTime)) return false;
        GtfsStopTime that = (GtfsStopTime) o;
        return getId() == that.getId() &&
                Objects.equals(getTripId(), that.getTripId()) &&
                Objects.equals(getArrivalTime(), that.getArrivalTime()) &&
                Objects.equals(getDepartureTime(), that.getDepartureTime()) &&
                Objects.equals(getStopId(), that.getStopId()) &&
                Objects.equals(getStopSequence(), that.getStopSequence()) &&
                Objects.equals(getPickupType(), that.getPickupType()) &&
                Objects.equals(getDropOffType(), that.getDropOffType()) &&
                Objects.equals(getDistance(), that.getDistance()) &&
                Objects.equals(getDate(), that.getDate());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTripId(), getArrivalTime(), getDepartureTime(), getStopId(), getStopSequence(), getPickupType(), getDropOffType(), getDistance(), getId(), getDate());
    }
}
