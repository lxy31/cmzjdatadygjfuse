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

/**
 * @program: cmzjdatadygjfuse
 * @description: 始终保持当前内存中存入的都是最新数据
 * @author: Mr.Deng -> Mr.Liu
 * @create: 2021-05-14 16:04
 **/
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
            /*整点清除前一小时数据*/
            if (flag && formatDate.substring(14, 16).equals("00")) {
                timeMap.entrySet().removeIf(entry -> Long.parseLong(entry.getValue()) < (System.currentTimeMillis() - (1000 * 60 * 60 * 24)));
                System.out.println(timeMap.size());
                flag = false;
            }
            /*将最新数据和相同数据最新数据添加到内存中*/
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
