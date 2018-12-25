package com.mapreduce.page.topup;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author sanduo
 * @date 2018/09/07
 */
public class PageTopupReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    TreeMap<PageCount, String> map = new TreeMap<PageCount, String>();// 自带排序功能

    /* reduce
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        // 逐行叠加计算
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        PageCount pageCount = new PageCount();
        pageCount.set(key.toString(), count);
        map.put(pageCount, null);
    }

    /* 计算完成 写出去
     * 求出前5位
     * 
     * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int topn = conf.getInt("top.n", 5);// 这一步的设计为了让参数动态变化
        int i = 0;
        for (Entry<PageCount, String> entry : map.entrySet()) {
            context.write(new Text(entry.getKey().getPage()), new IntWritable(entry.getKey().getCount()));
            i++;
            if (i == topn)
                return;
        }

    }
}
