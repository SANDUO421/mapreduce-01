package com.mapreduce.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner 接口（使用reducer实现分组聚合）：在map Task 阶段局部聚合（merge）
 * 
 * 处理map task的局部聚合
 * 
 * @author sanduo
 * @date 2018/09/27
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    /* 局部分组聚合
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
