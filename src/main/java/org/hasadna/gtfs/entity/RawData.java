package org.hasadna.gtfs.entity;

import org.hibernate.annotations.GenericGenerator;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class RawData {
/*
create table raw_data (
  id varchar(255) not null,
  date varchar(255),
  route_id varchar(255),
  siri_raw_data varchar(255),
  trip_id varchar(255),
  primary key (id)
) engine=InnoDB DEFAULT CHARSET=utf8;

 */
    private String siriRawData;
    private String routeId;
    private String date;    // without time! 2019-12-30
    private String tripId;

    @Id
    @GeneratedValue(generator="system-uuid")
    @GenericGenerator(name="system-uuid", strategy = "uuid")
    private String id;  // auto generated?

    public RawData() {
    }

    public RawData(String siriRawData, String routeId, String date, String tripId) {
        this.siriRawData = siriRawData;
        this.routeId = routeId;
        this.date = date;
        this.tripId = tripId;
    }

    public String getSiriRawData() {
        return siriRawData;
    }

    public void setSiriRawData(String siriRawData) {
        this.siriRawData = siriRawData;
    }

    public String getRouteId() {
        return routeId;
    }

    public void setRouteId(String routeId) {
        this.routeId = routeId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    @Override
    public String toString() {
        return "RawData{" +
                "siriRawData='" + siriRawData + '\'' +
                ", routeId='" + routeId + '\'' +
                ", date='" + date + '\'' +
                ", tripId='" + tripId + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
