package com.asiainfo.dygj.util;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/***********************************
 *@Desc TODO
 *@ClassName NumUtil
 *@Author DLX
 *@Data 2020/6/12 20:59
 *@Since JDK1.8
 *@Version 1.0
 ***********************************/
public class NumUtil {
    //10进制转16进制
    public static String encodeHEX(Integer numb){
        String hex= Integer.toHexString(numb);
        return hex;
    }
    //16进制转10进制
    public static String decodeHEX(String hexs){
        try{
            if (hexs.length()!=0){
                BigInteger bigint=new BigInteger(hexs, 16);
                String numb=bigint.intValue()+"";
                return numb;
            }else {
                return hexs;
            }
        }catch (Exception e){
            return "";
        }
    }
    //16进制转10进制
    public static int decodeIntHEX(String hexs){
        BigInteger bigint=new BigInteger(hexs, 16);
        int numb=bigint.intValue();
        return numb;
    }
    //判断字符串是否是数字(空值返回false)
    public static boolean isInteger(String str) {
        if (str.length()!=0 || !str.equals("")){
            Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
//            Pattern pattern = Pattern.compile("[0-9]*");
            return pattern.matcher(str).matches();
        }else {
            return false;
        }
    }

    public static void main(String[] args) {

        System.out.println(NumUtil.isInteger(""));
    }
}
