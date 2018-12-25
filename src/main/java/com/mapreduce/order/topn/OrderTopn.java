package com.mapreduce.order.topn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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
 * 分组聚合商品求订单的前三位
 * 
 * @author sanduo
 * @date 2018/09/13
 */
public class OrderTopn {

    public static class OrderTopnMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
        // 保证每次调都是同一个对象只是值不同
        OrderBean orderBean = new OrderBean();
        Text k = new Text();

        /* 分组
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(",");

            // OrderBean orderBean = new OrderBean();// 如果这样设计，每个mapperTask就会产生一个对象，此处改早
            orderBean.setOrderBean(words[0], words[1], words[2], Float.parseFloat(words[3]),
                Integer.parseInt(words[4]));

            // context.write(new Text(orderBean.getOrderId()), orderBean);
            k.set(words[0]);
            // 从这里交给maptask的kv对象，会被maptask序列化后存储，所以不用担心被覆盖
            context.write(k, orderBean);
        }
    }

    public static class OrderTopnReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {

        /* 聚合
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context)
            throws IOException, InterruptedException {

            // 获取topn参数
            int topn = context.getConfiguration().getInt("order.top.n", 3);

            // 取出每一组的数据存储的ArrayList
            ArrayList<OrderBean> list = new ArrayList<OrderBean>();// 将每一组的数据存储到ArrayList中
            for (OrderBean value : values) {
                OrderBean orderBean = new OrderBean();// 保证每次循环都是新的对象
                orderBean.setOrderBean(value.getOrderId(), value.getUserId(), value.getPdtName(), value.getPrice(),
                    value.getNum());
                list.add(orderBean);
            }

            // 对list中的OrderBean排序（按金额的总大小排序，如果金额相同，按照商品名称）

            // 对每组数据排序
            Collections.sort(list);// 实现了compartor

            for (int i = 0; i < topn; i++) {
                context.write(list.get(i), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("order.top.n", 3);

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderTopn.class);

        job.setMapperClass(OrderTopnMapper.class);
        job.setReducerClass(OrderTopnReducer.class);

        // 高效排序，使用框架自己定义的分发和分组规则

        // 设置分发规则使用类，默认是使用的HashPartitioner
        // job.setPartitionerClass(OrderIdPartitioner.class);
        // 修改分组策略
        // job.setGroupingComparatorClass(OrderIdGroupingComparator.class);

        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\order\\topn\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\order\\topn\\output"));

        System.out.println(job.waitForCompletion(true) ? "排序成功" : "排序失败");

    }

}
