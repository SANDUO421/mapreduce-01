package com.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WordCount mapper <br/>
 * KEYIN ：是map task 读取到数据的key的类型，是一行数据的其实偏移量Long <br/>
 * VALUEIN：是map task 读取到的数据的value的类型，是一行的内容String <br/>
 * 
 * KEYOUT：是用户的自定义map方法要返回的结果kv数据的key的类型，在wordcount逻辑中，我们需要返回的单词 String <br/>
 * VALUEOUT：是用户的自定义map方法要返回的结果kv数据的value的类型，在wordcount逻辑中，我们需要返回的整数 Integer <br/>
 * 
 * 
 * 但是 在MapReduce 中 ，map产生的数据需要传输给reduce，需要进行序列化 和反序列化，而jdk中的原生序列化机制产生的数据量比较冗余，就会导致MapReduce运行过程中效率低下。
 * 所以，Hadoop专门设计了自己的序列化机制，name，MapReduce中传输的数据类型就必须实现Hadoop自己的序列化接口
 * 
 * Hadoop 为jdk中常用的基本类型 Long String Integer Float 等数据类型封装了自己的实现了hadoop序列化接口类型，LongWritable，Text，IntWritable，FloatWritable
 * 
 * @author sanduo
 * @date 2018/09/04
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /* 重写map---我们的逻辑
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 切分单词
        String[] words = value.toString().split(" ");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
