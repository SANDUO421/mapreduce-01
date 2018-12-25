package com.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 获取每个文件中每个单词出现的单词数量<hello,文件名->1>
 * 
 * @author sanduo
 * @date 2018/09/11
 */
public class IndexStepTwo {

    /**
     * mapper 切分获取每个文件的单词数量
     * 
     * @author sanduo
     * @date 2018/09/11
     */
    public static class IndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        /* map 产生 <hello,文件名->1>
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("-");
            context.write(new Text(words[0]), new Text(words[1].replaceAll("\t", "->")));
        }
    }

    public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {

        // 一组数据 <hello,a.txt->4>,<hello,a.txt->2>，<hello,a.txt->3>
        /* reduce
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            // stringBuffer是线程安全的，StringBuilder不是线程安全的，不涉及到线程安全，StringBuilder更快
            StringBuilder sb = new StringBuilder();

            for (Text value : values) {
                sb.append(value.toString()).append("\t");
            }
            context.write(key, new Text(sb.toString()));

        }
    }
    // 提交job

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStepTwo.class);

        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\index\\output01"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\index\\output02"));
        System.out.println(job.waitForCompletion(true) ? "任务提交完成" : "job不运行失败");

    }

}
