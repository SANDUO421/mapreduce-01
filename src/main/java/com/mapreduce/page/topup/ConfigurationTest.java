package com.mapreduce.page.topup;

import org.apache.hadoop.conf.Configuration;

/**
 * 加载configuration 测试
 * 
 * @author sanduo
 * @date 2018/09/07
 */
public class ConfigurationTest {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.addResource("xxxx.xml");// 指定加载那个配置文件
        System.out.println(conf.get("top.n"));

    }

}
