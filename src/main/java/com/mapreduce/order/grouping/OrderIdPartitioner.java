package com.mapreduce.order.grouping;

import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分发策略：基于分组策略，认为orderid相同的bean分发给同一个reduce task（返回相同的分区号）
 * 
 * 基于orederId 进行分区
 * 
 * @author sanduo
 * @date 2018/09/14
 */
public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {
    // key:手机号前缀，value：归属地的编号
    static HashMap<String, Integer> codeMap = new HashMap<String, Integer>();

    /* 获取分区号
     * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
     */
    @Override
    public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
        // 按照订单中的orderId进行分区(hash 取模运算)
        return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

}
