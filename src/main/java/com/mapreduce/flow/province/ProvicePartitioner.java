package com.mapreduce.flow.province;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.mapreduce.flow.sum.FlowBean;

/**
 * 本类的作用是提供给MapTask使用的，MapTask通过该类的getpartition方法，计算它所产生的每一对kv数据该分发给哪一个reduce Task
 * 
 * Partitioner<KEY, VALUE>：
 * 
 * KEY : map 中输出的key类型 VALUE：map中输出value的类型
 * 
 * @author sanduo
 * @date 2018/09/11
 */
public class ProvicePartitioner extends Partitioner<Text, FlowBean> {
    // key:手机号前缀，value：归属地的编号
    static HashMap<String, Integer> codeMap = new HashMap<String, Integer>();

    /**
     * 将归属地的数据表全部加载到内存中，加载的时候会先执行静态代码块
     * 
     */
    static {
        // codeMap = AttributeJdbsUtils.loadTable();
        codeMap.put("135", 0);
        codeMap.put("136", 1);
        codeMap.put("137", 2);
        codeMap.put("138", 3);
        codeMap.put("139", 4);
        codeMap.put("150", 5);
        codeMap.put("159", 6);
        codeMap.put("182", 7);
        codeMap.put("183", 8);
    }

    /* 实现分发规则
     * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
     */
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        // 查询数据库获取，每个手机号是那个归属地,但是因为每个key-vlue都会分发，所以会频繁的调用数据库，
        // 分发数据的细节是mapTask完成，所以该程序会被很多的MapTask程序调用。会对数据库产生压力
        // 解决上面的问题：就是将归属地的数据加载到内存中

        // int code = AttributeJdbsUtils.getProvince(key.toString().substring(0, 7));

        Integer code = codeMap.get(key.toString().substring(0, 3));
        return code == null ? 9 : code;
    }

}
