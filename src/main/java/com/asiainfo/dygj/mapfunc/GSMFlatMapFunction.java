package com.asiainfo.dygj.mapfunc;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.NumUtil;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName GSMFlatMapFunction
 *@Author DLX
 *@Data 2020/8/13 15:56
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class GSMFlatMapFunction extends RichFlatMapFunction<String, SignalFormat> {
    @Override
    public void flatMap(String s, Collector<SignalFormat> collector) throws Exception {
//        0 1     2 3      4                                                               5 6       7 8       9 10 11         12         13              14         15 16              17         18 19             20  21 22 23 24 25 26 27  28  29   30  21  32 33 34       35 36 37       38   39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58       59 60              61 62 63 64 65 66 67 68         69 70   71      72 73 74 75 76 77 78 79 80 81             82       83                                                  84 85   86 87 88          89 90                   91             92 93 94 95 96 97 98                                  99 100 101 102 103 104 105 106 107 108 109 110                111 112 113 114 115 116 117 118 119 120 121            122 123             124                              125
//        1;50001;0;202008;53180093<40001074:53180093:40006212,42001009:53180093:40006212,>;;50001011;;70019172;;-1;7705147007;7705761296;460000192862874;13780171765;0;008618933902872;18933902872; ;20200813100634;324;324;0;0; 0; 14; 1;571;577;5771;571;577;-1; 0;40001074; 0; 0;42001009;1140; 0;  ; 0; 0;  ; 0; 0;  ;  ; 6; 6; 0; 0; 0;  ;  ;  ;  ;-1;10100014; 0;356720088514140;  ;10;20;20;10; 0;  ;8600000000;  ;67FF;2A388CC;00;  ;  ; 0;  ;  ;  ;  ;  ;20200813101225;20200813;VLTCG03_VTAS03_H_110103_20200813101208_02638105.dat; 0;5771; 0;  ;18933902872;  ;-2296800590908226012;20200813100634;  ;  ; 0; 2;  ;  ;hzpsbc10bhw.19e.a71e.20200813020624;  ;  0;   ;   ;   ;   ;   ;   ;   ;   ;   ;DR_GSM_WZ_20200813;   ;   ;   ;   ;   ;   ;  0;   ;   ;   ;20200813101229;   ;140960826654720;{45000061:0:6}{45000161:1140:54};
        try {
            String[] gsmStrs = s.split(";", -1);
            SignalFormat gsmSignalFormat = new SignalFormat();
            if (gsmStrs[30].equals("571")) {
                String tmp_lac = NumUtil.decodeHEX(gsmStrs[70]);
                String tmp_ci = NumUtil.decodeHEX(gsmStrs[71]);
                String _phone = gsmStrs[14];
                int len = _phone.length();
                if (len > 0) {
                    gsmSignalFormat.sourceType = "6";
                    gsmSignalFormat.procedureType = "1";
                    gsmSignalFormat.imei = "";
                    gsmSignalFormat.imsi = gsmStrs[13];
                    gsmSignalFormat.lac = tmp_lac;
                    gsmSignalFormat.switchId = "";
                    gsmSignalFormat.ci = tmp_ci;
                    gsmSignalFormat.lac_ci = tmp_lac + "_" + tmp_ci;
                    gsmSignalFormat.signalTime = TimeUtil.dateToStamp(gsmStrs[19], "yyyyMMddHHmmss");
                    gsmSignalFormat.lng = "";
                    gsmSignalFormat.lat = "";
                    gsmSignalFormat.coordinate = "";
                    gsmSignalFormat.city = "";
                    gsmSignalFormat.county = "";
                    if (len == 13 && _phone.substring(0, 2).equals("86")) {
                        gsmSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 15 && _phone.substring(0, 2).equals("86")) {
                        gsmSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 12 && _phone.substring(0, 2).equals("86")) {
                        gsmSignalFormat.phone = _phone.substring(2, len);
                    } else {
                        gsmSignalFormat.phone = _phone;
                    }
                }
            } else {
                String tmp_lac = NumUtil.decodeHEX(gsmStrs[70]);
                String tmp_ci = NumUtil.decodeHEX(gsmStrs[71].replace("\"", ""));
                String _phone = gsmStrs[14];
                int len = _phone.length();
                if (len > 0) {
                    gsmSignalFormat.sourceType = "6";
                    gsmSignalFormat.procedureType = "2";
                    gsmSignalFormat.imei = "";
                    gsmSignalFormat.imsi = gsmStrs[13];
                    gsmSignalFormat.lac = tmp_lac;
                    gsmSignalFormat.switchId = gsmStrs[31];
                    gsmSignalFormat.ci = tmp_ci;
                    gsmSignalFormat.lac_ci = tmp_lac + "_" + tmp_ci;
                    gsmSignalFormat.signalTime = TimeUtil.dateToStamp(gsmStrs[19], "yyyyMMddHHmmss");
                    gsmSignalFormat.lng = "";
                    gsmSignalFormat.lat = "";
                    gsmSignalFormat.coordinate = "";
                    gsmSignalFormat.city = "";
                    gsmSignalFormat.county = "";
                    if (len == 13 && _phone.substring(0, 2).equals("86")) {
                        gsmSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 15 && _phone.substring(0, 2).equals("86")) {
                        gsmSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 12 && _phone.substring(0, 2).equals("86")) {
                        gsmSignalFormat.phone = _phone.substring(2, len);
                    } else {
                        gsmSignalFormat.phone = _phone;
                    }
                }
            }
            collector.collect(gsmSignalFormat);
        } catch (Exception ex) {
            System.out.println("KafkaTopic:B_EBU_GSM_NEW(jc) ,可能无数据产生，或者Kafka数据格式有变化，数据格式：" + s + ",报错信息：" + ex.getMessage());
        }
    }
}
