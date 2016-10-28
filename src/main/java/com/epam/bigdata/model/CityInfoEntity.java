package com.epam.bigdata.model;

public class CityInfoEntity {


    private float lat;
    private float lon;

    public CityInfoEntity(){}

    public CityInfoEntity(float latitude, float longitude){
        this.lat = latitude;
        this.lon = longitude;
    }

    public float getLat() {
        return lat;
    }

    public void setLat(float lat) {
        this.lat = lat;
    }

    public float getLon() {
        return lon;
    }

    public void setLon(float lon) {
        this.lon = lon;
    }

}
