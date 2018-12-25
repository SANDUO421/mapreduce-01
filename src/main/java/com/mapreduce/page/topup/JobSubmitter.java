package com.mapreduce.page.topup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 任务调度
 * 
 * @author sanduo
 * @date 2018/09/07
 */
public class JobSubmitter {
    public static void main(String[] args) throws Exception {
        /**
         * 第四种方式：通过加载classpath下的core-site.xml,core-default.xml,hdfs-site.xml,hdfs-default.xml文件解析参数
         */
        Configuration conf = new Configuration();// 这一步就会加载xxx-site.xml

        /**
         * 第一种方式：通过代码设置参数
         */
        // conf.setInt("top.n", 5);
        /**
         * 第二种方式：通过Hadoop jar xxx.jar 7;
         */
        // conf.setInt("top.n", Integer.parseInt(args[0]));
        /**
         * 第三种方式：通过 加载配置文件
         */
        // Properties props = new Properties();
        // props.load(JobSubmitter.class.getClassLoader().getResourceAsStream("site.properteis"));
        // int topn = Integer.parseInt(props.getProperty("top.n"));
        // conf.setInt("top.n", topn));
        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitter.class);

        job.setMapperClass(PageTopupMapper.class);
        job.setReducerClass(PageTopupReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\pagetopup\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\pagetopup\\output"));

        boolean waitForCompletion = job.waitForCompletion(true);

        System.out.println(waitForCompletion ? "计算完成" : "计算失败");
    }

}
