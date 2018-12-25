package com.mapreduce.friends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 两两之间有共同好友，以及共同好友是谁？
 * 
 * @author sanduo
 * @date 2018/09/21
 */
public class CommonFriendsOne {

    /**
     * 
     * 
     * @author sanduo
     * @date 2018/09/21
     */
    public static class CommonFriendsOneMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        /*
         * 
         * 输入一行数据：A:B,C,D,F,E,O
         * 
         * 输出： B->A ,C->A, D->A, F->A, E->A, O->A
         * 
         * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object,
         *      org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 读一行数据
            String[] userAndFriends = value.toString().split(":");
            String user = userAndFriends[0];
            String[] friends = userAndFriends[1].split(",");
            v.set(user);
            for (String friend : friends) {
                k.set(friend);
                context.write(k, v);
            }

        }

    }

    public static class CommonFriendsOneReducer extends Reducer<Text, Text, Text, Text> {
        Text k = new Text();

        /* reduce
         * 
         * 一组数： B -> A E F D ...
         * 另一组数： E -> D H L Q ...
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        protected void reduce(Text friend, Iterable<Text> users, Context context)
            throws IOException, InterruptedException {

            // 对friends 排序
            ArrayList<String> userList = new ArrayList<String>();

            for (Text user : users) {
                userList.add(user.toString());
            }
            Collections.sort(userList);

            // 输出

            for (int i = 0; i < userList.size() - 1; i++) {
                for (int j = i + 1; j < userList.size(); j++) {
                    k.set(userList.get(i) + "-" + userList.get(j));
                    context.write(k, friend);
                }

            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendsOne.class);

        job.setMapperClass(CommonFriendsOneMapper.class);
        job.setReducerClass(CommonFriendsOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\mapreduce\\friends\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bigdata\\mapreduce\\friends\\output"));

        System.out.println(job.waitForCompletion(true) ? "完成" : "失败");
    }

}
