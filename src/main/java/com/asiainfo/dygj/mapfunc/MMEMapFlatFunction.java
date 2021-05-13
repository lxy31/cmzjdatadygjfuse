package com.asiainfo.dygj.mapfunc;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.NumUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName MMEMapFlatFunction
 *@Author DLX
 *@Data 2020/8/12 16:38
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class MMEMapFlatFunction extends RichFlatMapFunction<String, SignalFormat> {
    @Override
    public void flatMap(String s, Collector<SignalFormat> collector) throws Exception {
//        0 1   2   3 4                                5 6               7               8           9  10            11            12 13   14 15 16 17 18 19       20 21 22 23   24 25       26 27          28                                      29            30              31    32    33    34
//        2|203|579|5|31633064383037383030393931663030|6|460007933520716|867099041324744|13858950868|20|1597286096493|1597286096493| 0|0014|  |00|  |  |  |0ac05fce|  |  |  |01a3|10|d0920abc|  |10.43.0.133|2490:8928:721c:152c:0001:0001:19ab:e815|100.67.252.82|100.111.211.195|36412|36412|26769|04eed196|||CMNET|0||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
        try {
            String[] mmeStrs = s.split("[|]", -1);
            if (mmeStrs.length >= 167) {
                if (mmeStrs[10].length() == 13 && mmeStrs[11].length() == 13 && mmeStrs[8].length() > 0 && mmeStrs[33].length() > 0 && mmeStrs[34].length() > 0) {
                    String _phone = "";
                    int len = mmeStrs[8].length();
                    if (len == 13 && mmeStrs[8].substring(0, 2).equals("86")) {
                        _phone = mmeStrs[8].substring(2, len);
                    } else if (len == 15 && mmeStrs[8].substring(0, 2).equals("86")) {
                        _phone = mmeStrs[8].substring(2, len);
                    } else if (len == 12 && mmeStrs[8].substring(0, 2).equals("86")) {
                        _phone = mmeStrs[8].substring(2, len);
                    } else {
                        _phone = mmeStrs[8];
                    }
                    String sourceType = "1";
                    String procedureType = mmeStrs[9];
                    String phone = _phone;
                    String imei = mmeStrs[7];
                    String imsi = mmeStrs[6];
                    String lac = mmeStrs[33];
                    String ci = NumUtil.decodeHEX(mmeStrs[34]);
                    String lac_ci = lac + "_" + ci;
                    String switchId = "";
                    String signalTime = mmeStrs[10];
                    String lng = "";
                    String lat = "";
                    String coordinate = "";
                    String city = mmeStrs[2];
                    String county = "";
                    collector.collect(SignalFormat.of(sourceType, procedureType, phone, imei, imsi, lac, ci, lac_ci, switchId, signalTime, lng, lat, coordinate, city, county));
                }

            }
        } catch (Exception ex) {
            System.out.println("KafkaTopic:DACP_RT_SDTP_S1MME_0(zz) ,可能无数据产生，或者Kafka数据格式有变化，数据格式：" + s + ",报错信息：" + ex.getMessage());
        }
    }
}
