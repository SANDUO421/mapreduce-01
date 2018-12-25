package com.mapreduce.join;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mapreduce.page.topup.JobSubmitter;

/**
 * join算法
 * 
 * 实现类似于 select a.*,b.* from a join b on a.uid=b.uid;
 * 
 * @author sanduo
 * @date 2018/09/21
 */
public class ReduceSideJoin {

    public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {
        String fileName = null;
        Text k = new Text();
        JoinBean bean = new JoinBean();

        /* maptask在做数据处理时，会先调用一次setup()
         * 调完后对每一行返回调用map()
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, JoinBean>.Context context)
            throws IOException, InterruptedException {
            // 获取文件名
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            fileName = inputSplit.getPath().getName();

        }

        /* 
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context)
            throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            // 区分user表和order表
            if (fileName.startsWith("order")) {
                // 因为赋值会调对象的getset方法，所以这一块最好一个字符串（这也就是为什么很多大数据表里面是字符串null）
                bean.set(fields[0], fields[1], "NULL", -1, "NULL", "order");
            } else {
                bean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
            }
            k.set(bean.getUserId());
            context.write(k, bean);// 产生 key-value
        }
    }

    public static class ReduceSideJoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable> {

        /* reduce
         * 
         * 分组聚合了相同用户为一组
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(Text key, Iterable<JoinBean> beans, Context context)
            throws IOException, InterruptedException {
            ArrayList<JoinBean> orderList = new ArrayList<JoinBean>();
            JoinBean userBean = null;
            try {
                // 区分user表和order表数据
                for (JoinBean bean : beans) {
                    // 防止空指针异常
                    if ("order".equalsIgnoreCase(bean.getTableName())) {
                        JoinBean orderBean = new JoinBean();

                        BeanUtils.copyProperties(orderBean, bean);
                        orderList.add(orderBean);
                    } else {
                        userBean = new JoinBean();
                        BeanUtils.copyProperties(userBean, bean);
                    }
                }
                // 拼接数据，并输出
                for (JoinBean order : orderList) {

                    // 因为已经是一组了所以不需要在判断了
                    // if (order.getUserId().equals(userBean.getUserId())) {
                    order.setUserName(userBean.getUserName());
                    order.setUserAge(userBean.getUserAge());
                    order.setUserFriends(userBean.getUserFriends());
                    context.write(order, NullWritable.get());
                    // }

                }

            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();// 这一步就会加载xxx-site.xml

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitter.class);

        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setReducerClass(ReduceSideJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);

        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\join\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\join\\output"));

        boolean waitForCompletion = job.waitForCompletion(true);

        System.out.println(waitForCompletion ? "计算完成" : "计算失败");
    }

}
