package com.mapreduce.flow.sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mapreduce.flow.province.ProvicePartitioner;

/**
 * 任务调度类
 * 
 * @author sanduo
 * @date 2018/09/06
 */
public class JobSubmitter {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitter.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 设置分发规则使用类，默认是使用的HashPartitioner
        job.setPartitionerClass(ProvicePartitioner.class);

        // 因为分发规则的确定，所以必须制定需要几个mapreduce处理数据
        job.setNumReduceTasks(10);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\flowcount\\input"));
        // FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\flowcount\\output"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\flowcount\\province-output"));

        job.waitForCompletion(true);

    }

}
