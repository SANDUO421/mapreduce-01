package com.mapreduce.page.countsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author sanduo
 * @date 2018/09/07
 */
public class PageCountSortStep1 {

    // mapper
    public static class PgaeCountSort1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        /* mapper01
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(" ");

            context.write(new Text(line[1]), new IntWritable(1));
        }

    }

    // reducer
    public static class PgaeCountSort1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /* reducer01
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }

            context.write(key, new IntWritable(count));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageCountSortStep1.class);

        job.setMapperClass(PgaeCountSort1Mapper.class);
        job.setReducerClass(PgaeCountSort1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\pagecountsort\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\pagecountsort\\countout"));

        job.setNumReduceTasks(3);

        System.out.println(job.waitForCompletion(true) ? "统计完成！" : "统计失败！");
    }

}
