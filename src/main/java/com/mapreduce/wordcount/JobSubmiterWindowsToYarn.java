package com.mapreduce.wordcount;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用于提交mapreduce job的客户端程序<br/>
 * 功能： <br/>
 * 1.封装本次job运行时所需的必要参数<br/>
 * 2.跟yarn进行交互，将mapreduce程序成功的启动，运行
 * 
 * 运行前提条件：<br/>
 * 打成jar包，重命名，放到"d:/wc.jar"路径下
 * 
 * @author sanduo
 * @date 2018/09/04
 */
public class JobSubmiterWindowsToYarn {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        // 1.设置：获取job客户端
        Job job = getInstance();

        // 1.封装参数：job执行jar包
        job.setJar("d:/wc.jar"); // 这种方式无法动态获取
        // job.setJarByClass(JobSubmiterWindowsToYarn.class);// 根据该类所在的jar包，获取需要运行的mapreduce

        // 2.封装参数：本次job所要调用的Mapper实现类，Reducer实现类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 3.封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据key、value类型
        // mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4.封装参数：本次job要处理的输入数据所在路径，最后结果输出路径
        // 注意：导入org.apache.hadoop.mapreduce.lib.input.FileInputFormat下面的类
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));

        Path outPath = new Path("/wordcount/output");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), getConfig(), "root");
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);// 递归删除
        }
        FileOutputFormat.setOutputPath(job, outPath);// 注意此文件夹不能存在，否则抛异常

        // 5.封装参数：想要启动的Reduce task 的数量
        job.setNumReduceTasks(1);// 默认1

        // 6.提交job给yarn
        // job.submit();//这种提交方式不知道运行的状态和结果，不推荐使用
        boolean result = job.waitForCompletion(true);// 与resourcemanager一直连接，将运行进度相关信息打印到控制台，等待完成
        // 成功推出，返回0，否则是-1
        System.exit(result ? 0 : -1);// 为之后写脚本用

        if (result) {
            System.out.println("运行成功");
        } else {
            System.out.println("运行失败");
        }
    }

    /**
     * 封装job客户端
     * 
     * @return
     */
    private static Job getInstance() {
        Job job = null;
        try {
            Configuration conf = getConfig();
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    /**
     * 获取config
     * 
     * @return
     */
    private static Configuration getConfig() {
        Configuration conf = new Configuration();
        // 1. 设置job运行时需要访问的默认文件系统
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        // 2. 设置job提交到哪里运行
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "hadoop01");
        // 3.设置参数:如过要从windows系统上运行这个job提交客户端程序，则需要添加这个跨平台提交参数
        conf.set("mapreduce.app-submission.cross-platform", "true");
        return conf;
    }

}
