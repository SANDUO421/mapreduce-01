package com.mapreduce.flow.sum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 对每一个人的流量统计
 * 
 * @author sanduo
 * @date 2018/09/06
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    /* 手机号：key
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 切分获取手机号、上行流量、下行流量
        String[] fields = value.toString().split("\t");
        // 以手机号为key，将上行和下行流量发出去
        String phone = fields[1];
        int len = fields.length;
        int upFlow = Integer.parseInt(fields[len - 3]);
        int downFlow = Integer.parseInt(fields[len - 2]);

        context.write(new Text(phone), new FlowBean(phone, upFlow, downFlow));

    }
}
