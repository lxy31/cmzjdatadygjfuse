package com.asiainfo.dygj.filterfunc;

import com.asiainfo.dygj.bean.SignalFormat;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/***********************************
 *@Desc TODO
 *@ClassName PhoneTimeFlatMapFunction
 *@Author DLX
 *@Data 2020/8/25 17:07
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class PhoneTimeFlatMapFunction extends RichFlatMapFunction<SignalFormat, SignalFormat> {
    private Map<String,String> timeMap = new HashMap<String,String>();
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    boolean flag = false;
    @Override
    public void flatMap(SignalFormat signalFormat, Collector<SignalFormat> collector) throws Exception {
        String formatDate = df.format(new Date());
        if (!flag && (formatDate.substring(15, 16).equals("4") || formatDate.substring(15, 16).equals("9"))){
            flag = true;
        }
        //每五分钟插入一次
        if (flag && Integer.parseInt(formatDate.substring(14, 16)) % 5 == 0) {
            timeMap.clear();
            flag = false;
        }

        String phoneSaveTime = timeMap.get(signalFormat.phone);
        if (phoneSaveTime == null){
            timeMap.put(signalFormat.phone,signalFormat.signalTime);
            collector.collect(signalFormat);
        }else if (Long.parseLong(phoneSaveTime)>Long.parseLong(signalFormat.signalTime)){
        }else {
            timeMap.put(signalFormat.phone,signalFormat.signalTime);
            collector.collect(signalFormat);
        }
    }
}
