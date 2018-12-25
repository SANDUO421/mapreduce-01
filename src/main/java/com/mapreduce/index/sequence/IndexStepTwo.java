package com.mapreduce.index.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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
    public static class IndexStepTwoMapper extends Mapper<Text, IntWritable, Text, Text> {

        Text k = new Text();
        Text v = new Text();

        /* map 产生 <hello,文件名->1>
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split("-");
            k.set(words[0]);
            v.set(words[1] + "-->" + value);
            context.write(k, v);
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

        // 默认输入组件
        // job.setInputFormatClass(TextInputFormat.class);//默认的文件输入系统
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\index\\output-seq-01"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\index\\output-seq-02"));
        System.out.println(job.waitForCompletion(true) ? "任务提交完成" : "job不运行失败");

    }

}
