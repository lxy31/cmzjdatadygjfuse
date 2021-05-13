package com.asiainfo.dygj.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/***********************************
 *@Desc TODO
 *@ClassName TimeUtil
 *@Author DLX
 *@Data 2020/8/11 17:41
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class TimeUtil {
    //时间戳转时间格式
    public static String timeMillisToData(String time) {
        Date date = new Date();
        date.setTime(Long.parseLong(time));
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    }

    //时间戳转时间格式
    public static String timeMillisToFilePath(String time) {
        String format = "";
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH");
        try {
            date.setTime(Long.parseLong(time));
            format = dateFormat.format(date);
        } catch (Exception ex) {
            System.out.println("当前时间戳转换为时间有异常,异常时间为：" + time + "," + "异常转换格式：" + dateFormat + ",报错信息：" + ex.getMessage());
        }
        return format;
    }

    //将时间转换为时间戳
    public static String dateToStamp(String s, String dateFormat) {
        String str = "";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        try {
            Date date = simpleDateFormat.parse(s);
            long ts = date.getTime();
            str = String.valueOf(ts);
        } catch (Exception ex) {
            System.out.println("当前时间转换时间戳有异常,异常时间为：" + s + "," + "异常转换格式：" + dateFormat + ",报错信息：" + ex.getMessage());
        }
        return str;
    }

    public static Long dateToLongStamp(String s, String dateFormat) throws ParseException {
        long time = 0;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        try {
            Date date = simpleDateFormat.parse(s);
            time = date.getTime();
        } catch (Exception ex) {
            System.out.println("当前时间转换时间戳有异常,异常时间为：" + s + "," + "异常转换格式：" + dateFormat + ",报错信息：" + ex.getMessage());
        }
        return time;
    }

    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s, String dateFormat) {//"yyyy-MM-dd HH:mm:ss"
        String format = "";
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        try {
            Date date = new Date(Long.parseLong(s));
            format = sdf.format(date);
        } catch (Exception ex) {
            System.out.println("当前时间戳转换日期有异常,异常时间戳为：" + s + "," + "异常转换格式：" + dateFormat + ",报错信息：" + ex.getMessage());
        }
        return format;
    }

}
