package com.skydp.chaindata.realtime.utils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;


public class ConfigUtil {
    /**
     *
     * @param key:配置的key
     * @return 配置值(value)
     * 获取配置文件中的值
     */
    public static String getProperty(String key) {
        Properties pro = new Properties();
//        InputStream in = ConfigUtil.class.getClassLoader().getResourceAsStream("config.properties");
        InputStreamReader in = new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"));
        try {
            pro.load(in);
            return pro.getProperty(key);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

//    public static void main(String[] args) {
//        String bootstrap_servers = ConfigUtil.getProperty("BOOTSTRAP_SERVERS");
//        System.out.println(bootstrap_servers);
//    }
}
