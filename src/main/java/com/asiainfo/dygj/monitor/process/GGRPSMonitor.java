package com.asiainfo.dygj.monitor.process;

import com.asiainfo.dygj.monitor.Constant;
import com.asiainfo.dygj.monitor.OracleDBTools;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @ClassName GGRPSMonitor
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/5/12 10:06
 * @Version 1.0
 **/
public class GGRPSMonitor extends RichSinkFunction<String> {
    private final static String NOW_TIME = "yyyy-MM-dd HH:mm:ss";

    @Override
    public void invoke(String s, Context context) throws Exception {
        LocalDateTime time = LocalDateTime.now();
        String format = time.format(DateTimeFormatter.ofPattern(NOW_TIME));
        String[] split = s.split(";", -1);
        String sql = "insert into GGRPSNUM values (?, TO_TIMESTAMP(?, 'yyyy-mm-dd hh24:mi:ss'))";
        OracleDBTools.excute(sql, Constant.TEST, split[0]+split[1]+split[2]+split[3]+split[4]+split[5]+split[6]+split[7]+split[8]+split[9], format);
    }
}
