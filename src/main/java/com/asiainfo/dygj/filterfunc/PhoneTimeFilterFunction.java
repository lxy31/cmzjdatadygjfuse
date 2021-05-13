package com.asiainfo.dygj.filterfunc;

import com.asiainfo.dygj.bean.SignalFormat;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/***********************************
 *@Desc TODO
 *@ClassName PhoneTimeFilterFunction
 *@Author DLX
 *@Data 2020/8/19 15:03
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class PhoneTimeFilterFunction extends RichFilterFunction<SignalFormat> {
    private final Map<String, String> timeMap = new HashMap<>();
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    boolean flag = false;

    @Override
    public boolean filter(SignalFormat signalFormat) throws Exception {
        boolean resultFlag = false;
        String formatDate = df.format(new Date());
        try {
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
            if (phoneSaveTime == null) {
                timeMap.put(signalFormat.phone, signalFormat.signalTime);
                resultFlag = true;
            } else {
                if (Long.parseLong(phoneSaveTime) <= Long.parseLong(signalFormat.signalTime)) {
                    timeMap.put(signalFormat.phone, signalFormat.signalTime);
                    resultFlag = true;
                }
            }
        } catch (Exception ex) {
            System.out.println("当前过滤数据数据有问题，有问题数据为：" + signalFormat + "，报错信息：" + ex.getMessage());
        }
        return resultFlag;
    }
}
