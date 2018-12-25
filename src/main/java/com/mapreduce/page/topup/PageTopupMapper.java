package com.mapreduce.page.topup;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 统计访问量前五的网站
 * 
 * @author sanduo
 * @date 2018/09/07
 */
public class PageTopupMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /* mapper
     * 
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(" ");
        context.write(new Text(line[1]), new IntWritable(1));

    }
}
