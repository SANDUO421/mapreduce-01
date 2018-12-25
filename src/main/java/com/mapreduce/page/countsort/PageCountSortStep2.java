package com.mapreduce.page.countsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class PageCountSortStep2 {

    // mapper
    public static class PgaeCountSort2Mapper extends Mapper<LongWritable, Text, PageCount, NullWritable> {

        /* mapper02
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");

            PageCount pageCount = new PageCount();
            pageCount.set(line[0], Integer.parseInt(line[1]));

            context.write(pageCount, NullWritable.get());
        }

    }

    // reducer
    public static class PgaeCountSort2Reducer extends Reducer<PageCount, NullWritable, PageCount, NullWritable> {

        /* reducer02
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(PageCount key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageCountSortStep2.class);

        job.setMapperClass(PgaeCountSort2Mapper.class);
        job.setReducerClass(PgaeCountSort2Reducer.class);

        job.setMapOutputKeyClass(PageCount.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(PageCount.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\pagecountsort\\countout"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\pagecountsort\\sortout"));

        job.setNumReduceTasks(1);// 必须是一个，否则数据分散

        System.out.println(job.waitForCompletion(true) ? "统计完成！" : "统计失败！");
    }

}
