package com.asiainfo.dygj.mapfunc;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName ZXFlatMapFunction
 *@Author DLX
 *@Data 2020/8/13 15:09
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class ZXFlatMapFunction extends RichFlatMapFunction<String, SignalFormat> {
    @Override
    public void flatMap(String s, Collector<SignalFormat> collector) throws Exception {
//        0              1 2            3 4              5 6              7              8     9     10    11   12 13 14 15
//        460000853500065,,8613850883504,,869642003798880,,20200813100636,20200813100639,22674,65535,22683,62282,2, 0, 0,255
        try {
            String[] zxStrs = s.split(",", -1);
            SignalFormat zxSignalFormat = new SignalFormat();
            String _phone = "";
            if (zxStrs.length >= 16) {
                boolean submit_flag = false;
                if (zxStrs[12].equals("0") || zxStrs[12].equals("1")) {
                    if (zxStrs[13].equals("0")) {
                        zxSignalFormat.sourceType = "2";
                        zxSignalFormat.procedureType = "2";
                        _phone = zxStrs[2];
                        zxSignalFormat.imei = zxStrs[4];
                        zxSignalFormat.imsi = zxStrs[0];
                        zxSignalFormat.lac = zxStrs[10];
                        zxSignalFormat.ci = zxStrs[11];
                        zxSignalFormat.lac_ci = zxStrs[10] + "_" + zxStrs[11];
                        submit_flag = true;
                    } else if ("1".equals(zxStrs[13])) {
                        zxSignalFormat.sourceType = "2";
                        zxSignalFormat.procedureType = "2";
                        _phone = zxStrs[3];
                        zxSignalFormat.imei = zxStrs[5];
                        zxSignalFormat.imsi = zxStrs[1];
                        zxSignalFormat.lac = zxStrs[10];
                        zxSignalFormat.ci = zxStrs[11];
                        zxSignalFormat.lac_ci = zxStrs[10] + "_" + zxStrs[11];
                        submit_flag = true;
                    }
                } else if ("4".equals(zxStrs[12]) || "5".equals(zxStrs[12])) {
                    if ("0".equals(zxStrs[13])) {
                        zxSignalFormat.sourceType = "2";
                        zxSignalFormat.procedureType = "3";
                        _phone = zxStrs[2];
                        zxSignalFormat.imei = zxStrs[4];
                        zxSignalFormat.imsi = zxStrs[0];
                        zxSignalFormat.lac = zxStrs[8];
                        zxSignalFormat.ci = zxStrs[9];
                        zxSignalFormat.lac_ci = zxStrs[8] + "_" + zxStrs[9];
                        submit_flag = true;
                    } else if ("1".equals(zxStrs[13])) {
                        zxSignalFormat.sourceType = "2";
                        zxSignalFormat.procedureType = "3";
                        _phone = zxStrs[3];
                        zxSignalFormat.imei = zxStrs[5];
                        zxSignalFormat.imsi = zxStrs[1];
                        zxSignalFormat.lac = zxStrs[8];
                        zxSignalFormat.ci = zxStrs[9];
                        zxSignalFormat.lac_ci = zxStrs[8] + "_" + zxStrs[9];
                        submit_flag = true;
                    }
                } else if ("2".equals(zxStrs[12]) || "3".equals(zxStrs[12])) {
                    if ("3".equals(zxStrs[13])) {
                        zxSignalFormat.sourceType = "2";
                        zxSignalFormat.procedureType = "1";
                        _phone = zxStrs[2];
                        zxSignalFormat.imei = zxStrs[4];
                        zxSignalFormat.imsi = zxStrs[0];
                        zxSignalFormat.lac = zxStrs[10];
                        zxSignalFormat.ci = zxStrs[11];
                        zxSignalFormat.lac_ci = zxStrs[10] + "_" + zxStrs[11];
                        submit_flag = true;
                    } else if ("1".equals(zxStrs[13])) {
                        zxSignalFormat.sourceType = "2";
                        zxSignalFormat.procedureType = "5";
                        _phone = zxStrs[2];
                        zxSignalFormat.imei = zxStrs[4];
                        zxSignalFormat.imsi = zxStrs[0];
                        zxSignalFormat.lac = zxStrs[10];
                        zxSignalFormat.ci = zxStrs[11];
                        zxSignalFormat.lac_ci = zxStrs[10] + "_" + zxStrs[11];
                        //signalFormatTuple.signalTime=res[6];
                        submit_flag = true;
                    } else if ("0".equals(zxStrs[13]) || "2".equals(zxStrs[13])) {
                        zxSignalFormat.sourceType = "2";
                        zxSignalFormat.procedureType = "4";
                        _phone = zxStrs[2];
                        zxSignalFormat.imei = zxStrs[4];
                        zxSignalFormat.imsi = zxStrs[0];
                        zxSignalFormat.lac = zxStrs[10];
                        zxSignalFormat.ci = zxStrs[11];
                        zxSignalFormat.lac_ci = zxStrs[10] + "_" + zxStrs[11];
                        //signalFormatTuple.signalTime=res[6];
                        submit_flag = true;
                    }
                    zxSignalFormat.switchId = "";
                    zxSignalFormat.lng = "";
                    zxSignalFormat.lat = "";
                    zxSignalFormat.coordinate = "";
                    zxSignalFormat.city = "";
                    zxSignalFormat.county = "";

                }
                int len = _phone.length();
                if (submit_flag && len > 2 && !"0".equals(zxSignalFormat.lac)) {
                    zxSignalFormat.signalTime = TimeUtil.dateToStamp(zxStrs[6], "yyyyMMddHHmmss");

                    if (len == 13 && _phone.substring(0, 2).equals("86")) {
                        zxSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 15 && _phone.substring(0, 2).equals("86")) {
                        zxSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 12 && _phone.substring(0, 2).equals("86")) {
                        zxSignalFormat.phone = _phone.substring(2, len);
                    } else {
                        zxSignalFormat.phone = _phone;
                    }
                    collector.collect(zxSignalFormat);
                }
            }
        } catch (Exception ex) {
            System.out.println("KafkaTopic:ZC_CS_ZX_ZJ_PO01(jc) ,可能无数据产生，或者Kafka数据格式有变化，数据格式：" + s + ",报错信息：" + ex.getMessage());
        }
    }
}
