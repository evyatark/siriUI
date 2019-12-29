package org.hasadna.gtfs.entity;

import org.hibernate.annotations.GenericGenerator;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity(name="inside_raw")
public class InsideData {
/*
CREATE TABLE inside_raw (
                           log_date VARCHAR(30) NOT NULL,
                           description VARCHAR(255) NOT NULL,
                           agency VARCHAR(5) NOT NULL,
                           route_id VARCHAR(10) NOT NULL,
                           line_name VARCHAR(10) NOT NULL,
                           trip_id VARCHAR(10) NOT NULL,
                           oad VARCHAR(30) NOT NULL,
                           vehicle_id VARCHAR(20) NOT NULL,
                           eta VARCHAR(30) NOT NULL,
                           gps_date VARCHAR(30) NOT NULL,
                           gps_long VARCHAR(20) NOT NULL,
                           gps_lat VARCHAR(20) NOT NULL,
                           date VARCHAR(20) NOT NULL,
                           unknown VARCHAR(20) NOT NULL,
                           format_version VARCHAR(5) NOT NULL,
                           id INT NOT NULL AUTO_INCREMENT,
                           PRIMARY KEY (id)
);
create index drt on inside_raw (date, route_id, trip_id);
 */
    private String logDate;
    private String description;
    private String agency;
    private String routeId;
    private String lineName;
    private String tripId;
    private String oad;
    private String vehicleId;
    private String eta;
    private String gpsDate;
    private String gpsLong;
    private String gpsLat;
    private String date;
    private String unknown;
    private String formatVersion;

    @Id
    private int id;  // auto generated

    public InsideData() {
    }



    public String getLogDate() {
        return logDate;
    }

    public void setLogDate(String logDate) {
        this.logDate = logDate;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getAgency() {
        return agency;
    }

    public void setAgency(String agency) {
        this.agency = agency;
    }

    public String getRouteId() {
        return routeId;
    }

    public void setRouteId(String routeId) {
        this.routeId = routeId;
    }

    public String getLineName() {
        return lineName;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getOad() {
        return oad;
    }

    public void setOad(String oad) {
        this.oad = oad;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getEta() {
        return eta;
    }

    public void setEta(String eta) {
        this.eta = eta;
    }

    public String getGpsDate() {
        return gpsDate;
    }

    public void setGpsDate(String gpsDate) {
        this.gpsDate = gpsDate;
    }

    public String getGpsLong() {
        return gpsLong;
    }

    public void setGpsLong(String gpsLong) {
        this.gpsLong = gpsLong;
    }

    public String getGpsLat() {
        return gpsLat;
    }

    public void setGpsLat(String gpsLat) {
        this.gpsLat = gpsLat;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getUnknown() {
        return unknown;
    }

    public void setUnknown(String unknown) {
        this.unknown = unknown;
    }

    public String getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(String formatVersion) {
        this.formatVersion = formatVersion;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return  logDate +
                "," + description +
                ",'" + agency +
                "," + routeId +
                ",'" + lineName +
                "," + tripId +
                "," + oad +
                "," + vehicleId +
                "," + eta +
                "," + gpsDate +
                "," + gpsLong +
                "," + gpsLat +
                "," + date +
                "," + unknown +
                "," + formatVersion +
                "," + id;
    }


    public String toStringFull() {
        return "InsideData{" +
                "logDate='" + logDate + '\'' +
                ", description='" + description + '\'' +
                ", agency='" + agency + '\'' +
                ", routeId='" + routeId + '\'' +
                ", lineName='" + lineName + '\'' +
                ", tripId='" + tripId + '\'' +
                ", oad='" + oad + '\'' +
                ", vehicleId='" + vehicleId + '\'' +
                ", eta='" + eta + '\'' +
                ", gpsDate='" + gpsDate + '\'' +
                ", gpsLong='" + gpsLong + '\'' +
                ", gpsLat='" + gpsLat + '\'' +
                ", date='" + date + '\'' +
                ", unknown='" + unknown + '\'' +
                ", formatVersion='" + formatVersion + '\'' +
                ", id='" + id + '\'' +
                '}';
    }

}
