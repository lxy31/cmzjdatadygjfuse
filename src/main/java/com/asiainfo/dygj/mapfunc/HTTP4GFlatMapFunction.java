package com.asiainfo.dygj.mapfunc;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.NumUtil;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName HTTP4GFlatMapFunction
 *@Author DLX
 *@Data 2020/8/13 18:15
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class HTTP4GFlatMapFunction extends RichFlatMapFunction<String, SignalFormat> {
    @Override
    public void flatMap(String s, Collector<SignalFormat> collector) throws Exception {
//        0           1              2    3       4               5
//        18355896561,20200809153134,68A3,65EFD17,460027055853725,864373044356513
        try {
            String[] http4gStrs = s.split(",", -1);
            SignalFormat http4gSignalFormat = new SignalFormat();
            if (http4gStrs.length >= 6) {
                String tmp_lac = NumUtil.decodeHEX(http4gStrs[2]);
                String tmp_ci = NumUtil.decodeHEX(http4gStrs[3]);
                String _phone = http4gStrs[0];
                int len = _phone.length();
                if (len > 2) {
                    http4gSignalFormat.sourceType = "4";
                    http4gSignalFormat.procedureType = "1";
                    http4gSignalFormat.imei = "";
                    http4gSignalFormat.imsi = http4gStrs[4];
                    http4gSignalFormat.lac = tmp_lac;
                    http4gSignalFormat.ci = tmp_ci;
                    http4gSignalFormat.lac_ci = tmp_lac + "_" + tmp_ci;
                    http4gSignalFormat.switchId = "";
                    http4gSignalFormat.signalTime = TimeUtil.dateToStamp(http4gStrs[1], "yyyyMMddHHmmss");
                    http4gSignalFormat.lng = "";
                    http4gSignalFormat.lat = "";
                    http4gSignalFormat.coordinate = "";
                    http4gSignalFormat.city = "";
                    http4gSignalFormat.county = "";
                    if (len == 13 && _phone.substring(0, 2).equals("86")) {
                        http4gSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 15 && _phone.substring(0, 2).equals("86")) {
                        http4gSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 12 && _phone.substring(0, 2).equals("86")) {
                        http4gSignalFormat.phone = _phone.substring(2, len);
                    } else {
                        http4gSignalFormat.phone = _phone;
                    }
                    collector.collect(http4gSignalFormat);
                }
            }
        } catch (Exception ex) {
            System.out.println("KafkaTopic:http_4g_lac(jc) ,可能无数据产生，或者Kafka数据格式有变化，数据格式：" + s + ",报错信息：" + ex.getMessage());
        }
    }
}
