package com.asiainfo.dygj.compress;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/***********************************
 *@Desc TODO
 *@ClassName PhoneTimeIntervalFilterFunction
 *@Author DLX
 *@Data 2020/12/8 9:55
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class PhoneTimeIntervalFilterFunction extends RichFilterFunction<SignalFormat> {
    private final Map<String, String> timeMap = new HashMap<String, String>();
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    boolean flag = false;

    @Override
    public boolean filter(SignalFormat signalFormat) throws Exception {
        try {
            String formatDate = df.format(new Date());
            if (!flag && formatDate.substring(14, 16).equals("59")) {
                flag = true;
            }

            //每小时清理一次过期状态
            if (flag && formatDate.substring(14, 16).equals("00")) {
                //使用迭代器的remove()方法删除元素
                timeMap.entrySet().removeIf(entry -> Long.parseLong(entry.getValue()) < (System.currentTimeMillis() - (1000 * 60 * 60 * 24)));
                System.out.println(timeMap.size());
                flag = false;
            }

            String phoneSaveTime = timeMap.get(signalFormat.phone);
            //判断map中是否保存过该号码，如果未保存则将其添加到map中
            if (phoneSaveTime == null) {
                timeMap.put(signalFormat.phone, TimeUtil.dateToStamp(signalFormat.signalTime, "yyyyMMddHHmmss"));
                return true;
            }
            //当map中存在该号码时，去出之前保存的时间戳与新流入数据进行比对如果在原数据五分钟内流入则过滤，反之则输出并更新map中的状态
            if ((Long.parseLong(phoneSaveTime) + (1000 * 60 * 5)) > TimeUtil.dateToLongStamp(signalFormat.signalTime, "yyyyMMddHHmmss")) {
                return false;
            } else {
                timeMap.put(signalFormat.phone, TimeUtil.dateToStamp(signalFormat.signalTime, "yyyyMMddHHmmss"));
                return true;
            }
        } catch (Exception ex) {
            System.out.println("数据格式存在问题，请检查：" + signalFormat);
        }
        return false;
    }
}