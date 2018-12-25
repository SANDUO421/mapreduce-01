package com.mapreduce.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 按组统计
 * 
 * @author sanduo
 * @date 2018/09/04
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /* 
     * key：一组的key
     * values：key对应的一组数据的迭代器
     * context：上下文
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        int count = 0;

        // 获取一组数据的迭代器
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }

}
