package com.asiainfo.dygj.mapfunc;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.NumUtil;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName OTTFlatMapFunction
 *@Author DLX
 *@Data 2020/8/13 18:23
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class OTTFlatMapFunction extends RichFlatMapFunction<String, SignalFormat> {
    @Override
    public void flatMap(String s, Collector<SignalFormat> collector) throws Exception {
//        0               1               2           3    4       5             6             7                   8                   9         10        11  12    13
//        46002990100252686260804736379815990117735582CB8CF903159703108149915970310815302020-08-10 11:44:412020-08-10 11:44:4130.472607120.31532urigcj02571
        try {
            String[] ottStrs = s.split("\001", -1);
            SignalFormat ottSignalFormat = new SignalFormat();
            if (ottStrs.length >= 14) {
                String tmp_lac = NumUtil.decodeHEX(ottStrs[3]);
                String tmp_ci = NumUtil.decodeHEX(ottStrs[4]);
                if (!"4.9E-324".equals(ottStrs[10]) && !"4.9E-324".equals(ottStrs[9])) {
                    String _phone = ottStrs[2];
                    int len = _phone.length();
                    ottSignalFormat.imei = ottStrs[1];
                    ottSignalFormat.imsi = ottStrs[0];
                    ottSignalFormat.signalTime = TimeUtil.dateToStamp(ottStrs[7], "yyyy-MM-dd HH:mm:ss");
                    ottSignalFormat.lng = ottStrs[10];
                    ottSignalFormat.lat = ottStrs[9];
                    ottSignalFormat.coordinate = ottStrs[12];
                    ottSignalFormat.city = "";
                    ottSignalFormat.county = "";
                    ottSignalFormat.lac = tmp_lac;
                    ottSignalFormat.ci = tmp_ci;
                    ottSignalFormat.lac_ci = tmp_lac + "_" + tmp_ci;
                    ottSignalFormat.switchId = "";
                    ottSignalFormat.sourceType = "7";
                    ottSignalFormat.procedureType = "1";
                    if (len == 13 && _phone.substring(0, 2).equals("86")) {
                        ottSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 15 && _phone.substring(0, 2).equals("86")) {
                        ottSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 12 && _phone.substring(0, 2).equals("86")) {
                        ottSignalFormat.phone = _phone.substring(2, len);
                    } else {
                        ottSignalFormat.phone = _phone;
                    }
                    collector.collect(ottSignalFormat);
                }
            }
        } catch (Exception ex) {
            System.out.println("KafkaTopic:TOPIC_OTT(jc) ,可能无数据产生，或者Kafka数据格式有变化，数据格式：" + s + ",报错信息：" + ex.getMessage());
        }
    }
}
