package com.mapreduce.flow.sum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer
 * 
 * @author sanduo
 * @date 2018/09/06
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    /* 分组计算手机号的上下行流量和总流量
     * 
     * key：手机号
     * value：是所有访问的流量数据
     * 如：<135,flowBean>,<135,flowBean1>,<135,flowBean2>
     * 
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
        throws IOException, InterruptedException {

        int upSum = 0;
        int downSum = 0;

        for (FlowBean value : values) {
            upSum += value.getUpFlow();
            downSum += value.getDownFlow();
        }

        context.write(key, new FlowBean(key.toString(), upSum, downSum));
    }
}
