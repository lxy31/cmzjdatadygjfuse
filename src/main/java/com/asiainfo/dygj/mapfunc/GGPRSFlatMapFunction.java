package com.asiainfo.dygj.mapfunc;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.NumUtil;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/***********************************
 *@Desc TODO
 *@ClassName GGPRSFlatMapFunction
 *@Author DLX
 *@Data 2020/8/13 16:38
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class GGPRSFlatMapFunction extends RichFlatMapFunction<String, SignalFormat> {

    @Override
    public void flatMap(String s, Collector<SignalFormat> collector) throws Exception {
//        0 1    2     3      4                                     5 6        7 8        9  10         11         12       13              14          15               16  17  18   19  20  21 22             23   24    25 26                27       28 29   30 31 32 33 34 35 36 37 38 39                                                                                                      40                                                                                                                                                         41 42 43       44 45 46   47      48 49       50       51    52 53   54                               55 56 57   58 59    60 61 62  63 64 65            66 67 68 69 70 71                                              72         73      74       75 76 77 78 79 80 81 82 83 84 85 86 87 88 89      90       91             92   93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117                                                118            119      120   121 122 123 124 125 126 127 128 129 130 131 132            133 134       135 136 137 138 139 140  141                    142 143 144 145 146 147 148 149 150 151            152 153                                                                                      154             155                                156 157 158
//        1;8305;53001;202008;53001021<43020141:53001021:54000126,>; ;53001001; ;70019172;-1;7917378445;7920947602;69000450;460026670745324;18267094818;3557540720753243;571;579;5798;571;579;  ;20200813083036;6180;21836; 0;19999108555806053;43020141; 0;6180;  ; 0; 0;  ; 0; 0;  ; 0; 0;{7920947602,73539001,6033671,579,66005941777991,21836,20200805174841,20200901000000:2334544,10485760,1};{7920947602,8050,579,21836,20200801000000,20200901000000:5689256,209715200}{7920947602,73804032,579,21836,20200801000000,20200901000000:5689187,524288000}; 2; 1;DF673082; 0; 1;585E;64AD801; 1;68181029;DF673088;CMNET;  ;018d;240989286892005D0000000000000000; 2; 1;0400; 0;46002; 2;  ;105; 0; 2;HZSAEGW165BHW;  ;  ; 0; 0; 2;N1750000003,N1750000013,N2000000002,N2000000009;2000000002;1196978;21162438;  ;  ;  ; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0;1196978;21162438;20200813083036;6180;  ; 0; 0; 0; 0; 0; 0;  0;  0;  0;  0;  0;  0;  0;   ;  0;  0;   ;   ;   ;  1;   ;   ;   ;LTECG107_HZCG11_PGWCDR_20200813101338_11904536.dat;20200813101403;20200813;21836;   ;   ;   ;  2; 15;  3;   ;   ;   ;   ;   ;20200813083000;   ;471875278;   ;  4;   ;   ;   ;6180;DR_GGPRS_JH_8_20200813;   ;   ;   ;   ;   ;   ;  4;   ;   ;20200813101413;   ;{7920947602,43020141,70470003,579,20200801000000,20200901000000,0,0,5689183,0,0,0,21836};141544942207616;{6033671:21836:1}{43020141:0:21836};  ;   ;
        try {
            String[] ggprsStrs = s.split(";", -1);
            SignalFormat ggprsSignalFormat = new SignalFormat();
            if (ggprsStrs.length >= 156) {
                String tmp_lac = NumUtil.decodeHEX(ggprsStrs[46]);
                String tmp_ci = NumUtil.decodeHEX(ggprsStrs[47]);
                String _phone = ggprsStrs[14];
                int len = _phone.length();
                if (len > 0) {
                    if (ggprsStrs[19].equals("571")) {
                        ggprsSignalFormat.sourceType = "5";
                        ggprsSignalFormat.procedureType = "1";
                        ggprsSignalFormat.switchId = "";
                    } else {
                        ggprsSignalFormat.sourceType = "5";
                        ggprsSignalFormat.procedureType = "2";
                        ggprsSignalFormat.switchId = ggprsStrs[19];//VPLMN1
                    }
                    ggprsSignalFormat.imei = ggprsStrs[15];
                    ggprsSignalFormat.imsi = ggprsStrs[13];
                    ggprsSignalFormat.lac = tmp_lac;
                    ggprsSignalFormat.ci = tmp_ci;
                    ggprsSignalFormat.lac_ci = tmp_lac + "_" + tmp_ci;
//                22             23
//                20200813083036;6180
                    if (NumUtil.isInteger(ggprsStrs[23])) {
                        ggprsSignalFormat.signalTime = (Long.parseLong(TimeUtil.dateToStamp(ggprsStrs[22], "yyyyMMddHHmmss")) + Long.parseLong(ggprsStrs[23])) + "";
                    } else {
                        ggprsSignalFormat.signalTime = TimeUtil.dateToStamp(ggprsStrs[22], "yyyyMMddHHmmss");
                    }
                    ggprsSignalFormat.lng = "";
                    ggprsSignalFormat.lat = "";
                    ggprsSignalFormat.coordinate = "";
                    ggprsSignalFormat.city = "";
                    ggprsSignalFormat.county = "";
                    if (len == 13 && _phone.substring(0, 2).equals("86")) {
                        ggprsSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 15 && _phone.substring(0, 2).equals("86")) { //13位物联网号码，号段取前8位
                        ggprsSignalFormat.phone = _phone.substring(2, len);
                    } else if (len == 12 && _phone.substring(0, 2).equals("86")) {
                        ggprsSignalFormat.phone = _phone.substring(2, len);
                    } else {
                        ggprsSignalFormat.phone = _phone;
                    }
                    collector.collect(ggprsSignalFormat);
                }
            }
        } catch (Exception ex) {
            System.out.println("KafkaTopic:topic_ggprs(jc) ,可能无数据产生，或者Kafka数据格式有变化，数据格式：" + s + ",报错信息：" + ex.getMessage());
        }
    }
}
