package org.hasadna.gtfs.entity;

public class StopData {
    public String stop_id ;
    public String stop_code;
    public String stop_name;
    public String stop_desc;
    public String stop_lat;
    public String stop_lon;
    public String location_type;
    public String parent_station;
    public String zone_id;

    public StopData(String stop_id, String stop_code, String stop_name, String stop_desc, String stop_lat, String stop_lon, String location_type, String parent_station, String zone_id) {
        this.stop_id = stop_id;
        this.stop_code = stop_code;
        this.stop_name = stop_name;
        this.stop_desc = stop_desc;
        this.stop_lat = stop_lat;
        this.stop_lon = stop_lon;
        this.location_type = location_type;
        this.parent_station = parent_station;
        this.zone_id = zone_id;
    }

    public static StopData extractFrom(final String line) {
        String[] data = line.split(",");
        return new StopData(
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]
        );
    }

    @Override
    public String toString() {
        return "StopData{" +
                "stop_id='" + stop_id + '\'' +
                ", stop_code='" + stop_code + '\'' +
                ", stop_name='" + stop_name + '\'' +
                ", stop_desc='" + stop_desc + '\'' +
                ", stop_lat='" + stop_lat + '\'' +
                ", stop_lon='" + stop_lon + '\'' +
                ", location_type='" + location_type + '\'' +
                ", parent_station='" + parent_station + '\'' +
                ", zone_id='" + zone_id + '\'' +
                '}';
    }
}
