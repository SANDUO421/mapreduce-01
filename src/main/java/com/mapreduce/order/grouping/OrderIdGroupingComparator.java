package com.mapreduce.order.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组时用的比较器
 * 
 * 修改key的分组策略（符合某种策略就是一组）
 * 
 * @author sanduo
 * @date 2018/09/14
 */
public class OrderIdGroupingComparator extends WritableComparator {

    /**
     * 注意：
     * 
     * 设计反序列化，告诉你需要序列化成那个类
     */
    public OrderIdGroupingComparator() {
        // 注意：调用父类的构造函数实例化，否则会报错
        super(OrderBean.class, true);
    }

    /* key的规则
     * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean)a;
        OrderBean o2 = (OrderBean)b;
        // orderId相同就分为一组
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
