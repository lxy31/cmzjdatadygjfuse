package com.asiainfo.dygj.monitor;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author Liu xiaoyu
 * @Date 2020/12/12 13:51
 * @Version 1.0
 * @describe:
 */
public class PropertiesUtils {
    private final static Properties prop;

    static {
        prop = new Properties();
        try {
            prop.load(Thread.currentThread()
                    .getContextClassLoader().getResourceAsStream("recommend.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }
}
