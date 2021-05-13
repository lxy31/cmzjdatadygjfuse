package com.asiainfo.dygj.bean;

/***********************************
 *@Desc TODO
 *@ClassName SignalFormat
 *@Author DLX
 *@Data 2020/8/11 10:15
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class SignalFormat {
    public String sourceType = "";
    public String procedureType = "";
    public String phone = "";
    public String imei = "";
    public String imsi = "";
    public String lac = "";
    public String ci = "";
    public String lac_ci = "";
    public String switchId = "";
    public String signalTime = "";
    public String lng = "";
    public String lat = "";
    public String coordinate = "";
    public String city = "";
    public String county = "";
    public String nowTime = "";

    public SignalFormat() {
    }

    public SignalFormat(String sourceType, String procedureType, String phone, String imei, String imsi, String lac, String ci, String lac_ci, String switchId, String signalTime, String lng, String lat, String coordinate, String city, String county) {
        this.sourceType = sourceType;
        this.procedureType = procedureType;
        this.phone = phone;
        this.imei = imei;
        this.imsi = imsi;
        this.lac = lac;
        this.ci = ci;
        this.lac_ci = lac_ci;
        this.switchId = switchId;
        this.signalTime = signalTime;
        this.lng = lng;
        this.lat = lat;
        this.coordinate = coordinate;
        this.city = city;
        this.county = county;
    }

    public String getNowTime() {
        return nowTime;
    }

    public void setNowTime(String nowTime) {
        this.nowTime = nowTime;
    }

    public static SignalFormat of(String sourceType, String procedureType, String phone, String imei, String imsi, String lac, String ci, String lac_ci, String switchId, String signalTime, String lng, String lat, String coordinate, String city, String county){
        return new SignalFormat(sourceType,procedureType,phone,imei,imsi,lac,ci,lac_ci,switchId,signalTime,lng,lat,coordinate,city,county);
    }

    @Override
    public String toString() {
        return sourceType + ',' +
               procedureType + ',' +
               phone + ',' +
               imei + ',' +
               imsi + ',' +
               lac + ',' +
               ci + ',' +
               lac_ci + ',' +
               switchId + ',' +
               signalTime + ',' +
               lng + ',' +
               lat + ',' +
               coordinate + ',' +
               city + ',' +
               county ;
    }


    public String downloadDataToString() {
        return sourceType + ',' +
                procedureType + ',' +
                phone + ',' +
                imei + ',' +
                imsi + ',' +
                lac + ',' +
                ci + ',' +
                switchId + ',' +
                signalTime + ',' +
                city + ',' +
                county ;
    }
}
