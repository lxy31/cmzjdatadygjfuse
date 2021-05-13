package com.asiainfo.dygj.mapfunc;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName NDFlatMapFunction
 *@Author DLX
 *@Data 2020/8/12 17:33
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class NDFlatMapFunction extends RichFlatMapFunction<String, SignalFormat> {
    @Override
    public void flatMap(String s, Collector<SignalFormat> collector) throws Exception {
//        0                   1             2               3     4         5         6       7
//        2020-08-13 10:15:07,8618357536001,460025575144134,26454,105524994,120.83164,29.5876,
        try {
            String[] ndStrs = s.split(",", -1);
            if (ndStrs.length >= 8 && ndStrs[7].length() > 0) {
                String _phone = ndStrs[1];
                int len = _phone.length();
                String imei = "";
                String imsi = ndStrs[2];
                String signalTime = TimeUtil.dateToStamp(ndStrs[0], "yyyy-MM-dd HH:mm:ss");
                String lng = "";
                String lat = "";
                String coordinate = "";
                String city = "";
                String county = "";
                String lac = "";
                String ci = "";
                String lac_ci = "";
                String switchId = ndStrs[7];
                String sourceType = "3";
                String procedureType = "1";
                String phone = null;
                if (len == 13 && _phone.substring(0, 2).equals("86")) {
                    phone = _phone.substring(2, len);
                } else if (len == 15 && _phone.substring(0, 2).equals("86")) {
                    phone = _phone.substring(2, len);
                } else if (len == 12 && _phone.substring(0, 2).equals("86")) {
                    phone = _phone.substring(2, len);
                } else {
                    phone = _phone;
                }
                collector.collect(SignalFormat.of(sourceType, procedureType, phone, imei, imsi, lac, ci, lac_ci, switchId, signalTime, lng, lat, coordinate, city, county));
            }
        } catch (Exception ex) {
            System.out.println("KafkaTopic:ZC_CS_ND_ZJ_PO01(jc) ,可能无数据产生，或者Kafka数据格式有变化，数据格式：" + s + ",报错信息：" + ex.getMessage());
        }
    }
}
