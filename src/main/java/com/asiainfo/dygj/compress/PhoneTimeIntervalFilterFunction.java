package com.asiainfo.dygj.compress;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.util.TimeUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @program: cmzjdatadygjfuse
 * @description: 剔除Topic：MULTIPLE_SOURCE_FUSION 5分钟内数据
 * @author: Mr.Deng -> Mr.Liu
 * @create: 2021-05-14 16:04
 **/
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
            /*整点剔除一天前的数据*/
            if (flag && formatDate.substring(14, 16).equals("00")) {
                timeMap.entrySet().removeIf(entry -> Long.parseLong(entry.getValue()) < (System.currentTimeMillis() - (1000 * 60 * 60 * 24)));
                System.out.println(timeMap.size());
                flag = false;
            }
            String phoneSaveTime = timeMap.get(signalFormat.phone);
            if (phoneSaveTime == null) {
                timeMap.put(signalFormat.phone, TimeUtil.dateToStamp(signalFormat.signalTime, "yyyyMMddHHmmss"));
                return true;
            }
            /*保证相同数据之间相差大于5分钟*/
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