package com.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mapreduce.combiner.WordCountCombiner;

/**
 * 开发中使用--本地模拟-运行在本地模式
 * 
 * @author sanduo
 * @date 2018/09/06
 */
public class JobSubmiterWindowsLocal {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 没指定默认文件系统---本地目录
        conf.set("fs.defaultFS", "file:///");
        // 没指定MapReduce-job提交到local运行
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitterLinuxToYarn.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // Combiner(mapTask端的局部聚合逻辑类)
        job.setCombinerClass(WordCountCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // FileInputFormat.setInputPaths(job, new Path("E:/bigdata/mapreduce/wordcount/input"));
        // FileOutputFormat.setOutputPath(job, new Path("E:/bigdata/mapreduce/wordcount/output"));
        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\wordcount\\combiner\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\wordcount\\combiner\\output"));

        job.setNumReduceTasks(1);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : -1);
    }

}
