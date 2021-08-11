package com.jxt.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * 常量类
 */
public class Constants {
    // 连接常量
    public static Configuration CONFIGURATION = HBaseConfiguration.create();
}
