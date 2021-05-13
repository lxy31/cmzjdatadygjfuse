package com.asiainfo.dygj.monitor.process;

import com.asiainfo.dygj.bean.SignalFormat;
import com.asiainfo.dygj.monitor.Constant;
import com.asiainfo.dygj.monitor.OracleDBTools;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @ClassName MyMonitor
 * @Description //TODO
 * @Author 刘晓雨
 * @Date 2021/3/8 15:54
 * @Version 1.0
 **/
public class FusionMonitor extends RichSinkFunction<SignalFormat> {
    private final static String NOW_TIME = "yyyy-MM-dd HH:mm:ss";

    @Override
    public void invoke(SignalFormat s, Context context) throws Exception {
        LocalDateTime time = LocalDateTime.now();
        String format = time.format(DateTimeFormatter.ofPattern(NOW_TIME));
        s.setNowTime(format);

        String sql = "insert into fusionNum(sourceType,procedureType,phone,imei,imsi,lac,ci,lac_ci,switchId,signalTime,lng,lat,coordinate,city,county,nowTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_timestamp(?,'yyyy-mm-dd hh24:mi:ss'))";
        OracleDBTools.excutemonitor(sql, Constant.TEST, s.sourceType, s.procedureType, s.phone, s.imei, s.imsi, s.lac, s.ci, s.lac_ci, s.switchId, s.signalTime, s.lng, s.lat, s.coordinate, s.city, s.county, s.getNowTime());
    }
}
