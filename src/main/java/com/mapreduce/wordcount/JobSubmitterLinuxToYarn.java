package com.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * linux 上运行的job客户端
 * 
 * 
 * 只有机器上配置了mapred-site.xml :才可以不配置:<br/>
 * conf.set("mapreduce.framework.name", "yarn");// 不配置使用Hadoop jar 会在本地模拟器运行
 * 
 * 
 * 如果要在Hadoop集群的某台机器上启动这个job提交客户端的话，conf里面 不需要指定fs.defaultFS mapreduce.framework.name
 * 
 * 因为在集群上用 hadoop jar xx.jar com.mapreduce.wordcount.JobSubmitterLinux 命令来启动客户端main方法时 Hadoop jar
 * 用这个命令将所在机器上的Hadoop安装目录中的jar和配置文件加入到运行时的classpath
 * 
 * 那么，我们的客户端main方法中的new Configuration() 语句就会加载classpath中的配置文件，自然就有了 fs.defaultFS和mapreduce.framework.name 和
 * yarn.resourcemanager.hostname 这些参数配置
 * 
 * @author sanduo
 * @date 2018/09/05
 */
public class JobSubmitterLinuxToYarn {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 没指定默认文件系统
        // 没指定MapReduce-job提交到哪里运行

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitterLinuxToYarn.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        job.setNumReduceTasks(3);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : -1);

    }

}
