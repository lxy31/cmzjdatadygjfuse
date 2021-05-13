package com.asiainfo.dygj.bean;

/***********************************
 *@Desc TODO
 *@ClassName BaseStationInfo
 *@Author DLX
 *@Data 2020/8/17 16:15
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class BaseStationInfo {
    public String lac_ci;
    public String city_id;
    public String county_id;

    public BaseStationInfo() {
    }

    public BaseStationInfo(String lac_ci, String city_id, String county_id) {
        this.lac_ci = lac_ci;
        this.city_id = city_id;
        this.county_id = county_id;
    }

    public static BaseStationInfo of(String lac_ci, String city_id, String county_id){
        return new BaseStationInfo(lac_ci,city_id,county_id);
    }

    @Override
    public String toString() {
        return "BaseStationInfo{" +
                "lac_ci='" + lac_ci + '\'' +
                ", city_id='" + city_id + '\'' +
                ", county_id='" + county_id + '\'' +
                '}';
    }
}
