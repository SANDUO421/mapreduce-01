
# MapReduce编程和开发



## WordCount 实践1

---
1.	定义mapper
2.	定义reducer
3.	定义job调用mapper和reduer

---


### mapper

```
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

```

### reducer


```
package com.mapreduce.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 按组统计
 * 
 * @author sanduo
 * @date 2018/09/04
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /* 
     * key：一组的key
     * values：key对应的一组数据的迭代器
     * context：上下文
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        int count = 0;

        // 获取一组数据的迭代器
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }

}

```


### 提交方式1-job(windows)---提交给yarn运行

```
package com.mapreduce.wordcount;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用于提交mapreduce job的客户端程序<br/>
 * 功能： <br/>
 * 1.封装本次job运行时所需的必要参数<br/>
 * 2.跟yarn进行交互，将mapreduce程序成功的启动，运行
 * 
 * 运行前提条件：<br/>
 * 打成jar包，重命名，放到"d:/wc.jar"路径下
 * 
 * @author sanduo
 * @date 2018/09/04
 */
public class JobSubmmiter {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        // 1.设置：获取job客户端
        Job job = getInstance();

        // 1.封装参数：job执行jar包
        job.setJar("d:/wc.jar"); // 这种方式无法动态获取
        // job.setJarByClass(JobSubmmiter.class);// 根据该类所在的jar包，获取需要运行的mapreduce

        // 2.封装参数：本次job所要调用的Mapper实现类，Reducer实现类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 3.封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据key、value类型
        // mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4.封装参数：本次job要处理的输入数据所在路径，最后结果输出路径
        // 注意：导入org.apache.hadoop.mapreduce.lib.input.FileInputFormat下面的类
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));

        Path outPath = new Path("/wordcount/output");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), getConfig(), "root");
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);// 递归删除
        }
        FileOutputFormat.setOutputPath(job, outPath);// 注意此文件夹不能存在，否则抛异常

        // 5.封装参数：想要启动的Reduce task 的数量
        job.setNumReduceTasks(1);// 默认1

        // 6.提交job给yarn
        // job.submit();//这种提交方式不知道运行的状态和结果，不推荐使用
        boolean result = job.waitForCompletion(true);// 与resourcemanager一直连接，将运行进度相关信息打印到控制台，等待完成
        // 成功推出，返回0，否则是-1
        System.exit(result ? 0 : -1);// 为之后写脚本用

        if (result) {
            System.out.println("运行成功");
        } else {
            System.out.println("运行失败");
        }
    }

    /**
     * 封装job客户端
     * 
     * @return
     */
    private static Job getInstance() {
        Job job = null;
        try {
            Configuration conf = getConfig();
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    /**
     * 获取config
     * 
     * @return
     */
    private static Configuration getConfig() {
        Configuration conf = new Configuration();
        // 1. 设置job运行时需要访问的默认文件系统
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        // 2. 设置job提交到哪里运行
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "hadoop01");
        // 3.设置参数:如过要从windows系统上运行这个job提交客户端程序，则需要添加这个跨平台提交参数
        conf.set("mapreduce.app-submission.cross-platform", "true");
        return conf;
    }

}

```

### 提交方式2-job(linux)---提交给yarn运行

```
package com.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * linux 上运行的job客户端
 * 
 * 
 * 只有机器上配置了mapred-site.xml :才可以不配置:<br/>
 * conf.set("mapreduce.framework.name", "yarn");// 不配置使用Hadoop jar 会在本地模拟器运行
 * 
 * 
 * 如果要在Hadoop集群的某台机器上启动这个job提交客户端的话，conf里面 不需要指定fs.defaultFS mapreduce.framework.name
 * 
 * 因为在集群上用 hadoop jar xx.jar com.mapreduce.wordcount.JobSubmitterLinux 命令来启动客户端main方法时 Hadoop jar
 * 用这个命令将所在机器上的Hadoop安装目录中的jar和配置文件加入到运行时的classpath
 * 
 * 那么，我们的客户端main方法中的new Configuration() 语句就会架子啊classpath中的配置文件，自然就有了 fs.defaultFS和mapreduce.framework.name 和
 * yarn.resourcemanager.hostname 这些参数配置
 * 
 * @author sanduo
 * @date 2018/09/05
 */
public class JobSubmitterLinux {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 没指定默认文件系统
        // 没指定MapReduce-job提交到哪里运行

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitterLinux.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        job.setNumReduceTasks(3);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : -1);

    }

}

```

### 提交方式3-job(linux)---开发中本地模式

```
package com.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 开发中使用--本地模拟-运行在本地模式
 * 
 * @author sanduo
 * @date 2018/09/06
 */
public class JobSubmiterWindowsLocal {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 没指定默认文件系统---本地目录
        conf.set("fs.defaultFS", "file:///");
        // 没指定MapReduce-job提交到哪里运行
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitterLinuxToYarn.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:/bigdata/mapreduce/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("E:/bigdata/mapreduce/wordcount/output"));

        job.setNumReduceTasks(3);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : -1);
    }

}

```

## FlowCount 实践2

---

flow.log

1.	统计flow.log中每个人访问网站所耗费的上行流量和下行流量以及总流量
2.	结果： 人  上行总流量  下行总流量  总流量	

---


### 自定义数据类型如何实现Hadoop的序列化接口

**注意**
	1.	自定义的对象需要实现Hadoop的序列化接口 Writable 
	2.	该类必须保持空参构造
	3.	write方法中输出字段二进制数据的顺序要与 readFields方法读取数据的顺序一致
	
```
package com.mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 流量封装
 * 
 * @author sanduo
 * @date 2018/09/06
 */
public class FlowBean implements Writable {

    /**
     * 手机号
     */

    private String phone;
    /**
     * 上行流量
     */
    private int upFlow;
    /**
     * 下行流量
     */
    private int downFlow;
    /**
     * 总流量
     */
    private int amountFlow;

    /**
     * @param upFLow
     * @param downFlow
     * @param amountFlow
     */
    public FlowBean(String phone, int upFlow, int downFlow) {
        super();
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.amountFlow = upFlow + downFlow;
    }

    /**
     * @return the phone
     */
    public String getPhone() {
        return phone;
    }

    /**
     * @param phone the phone to set
     */
    public void setPhone(String phone) {
        this.phone = phone;
    }

    /**
     * @return the upFLow
     */
    public int getUpFlow() {
        return upFlow;
    }

    /**
     * @param upFLow the upFLow to set
     */
    public void setUpFLow(int upFLow) {
        this.upFlow = upFLow;
    }

    /**
     * @return the downFlow
     */
    public int getDownFlow() {
        return downFlow;
    }

    /**
     * @param downFlow the downFlow to set
     */
    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    /**
     * @return the amountFlow
     */
    public int getAmountFlow() {
        return amountFlow;
    }

    /**
     * @param amountFlow the amountFlow to set
     */
    public void setAmountFlow(int amountFlow) {
        this.amountFlow = amountFlow;
    }

    /**
     * 必须要有
     */
    public FlowBean() {}

    /* 
     *  hadoop系统在反序列化该类的对象是要调用的方法---把对象的数据读入：赋值给对象
     *  注意:按照顺序
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.phone = in.readUTF();
        this.downFlow = in.readInt();
        this.amountFlow = in.readInt();
    }

    /*
     *
     * hadoop系统在序列化该类的对象是要调用的方法---把对象的数据发出去：把数据变成二进制放入其中就OK
     * 
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeUTF(phone);
        out.writeInt(downFlow);
        out.writeInt(amountFlow);
        // out.write(phone.getBytes());//这种方式接受的时候，不知道字符串有几个字节-容易报错

    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "upFlow=" + upFlow + ", downFlow=" + downFlow + ", amountFlow=" + amountFlow;
    }

}

```
## topup 实践3（）

---

request.dat
需求：
1.	统计页面访问前五的网站

分析：
1.	缓存到成员变量
2.	排序HashMap	
3.	只能一个reduceTask实现，因为每个reduce 有一个cleanup()
4.	在cleanup()中奖map中的数据排序输出（或者直接使用treemap
5. 	Treemap 怎么比大小，必须给规则：两种方式：一种是在数据类型中写一个compareto(),一种方式是给框架传递一个比较器comparator
6. 类型实现一个comparable接口就可以

---


### 参数文件加载的4种方式

```
/**
         * 第四种方式：通过加载classpath下的core-site.xml,core-default.xml,hdfs-site.xml,hdfs-default.xml文件解析参数
         */
        Configuration conf = new Configuration();// 这一步就会加载xxx-site.xml

        /**
         * 第一种方式：通过代码设置参数
         */
        // conf.setInt("top.n", 5);
        /**
         * 第二种方式：通过Hadoop jar xxx.jar 7;
         */
        // conf.setInt("top.n", Integer.parseInt(args[0]));
        /**
         * 第三种方式：通过 加载配置文件
         */
        // Properties props = new Properties();
        // props.load(JobSubmitter.class.getClassLoader().getResourceAsStream("site.properteis"));
        // int topn = Integer.parseInt(props.getProperty("top.n"));
        // conf.setInt("top.n", topn));
```


### 配置文件的3种加载方式

1.	xxx.properteis
2.	core-site.xml,core-default.xml,hdfs-site.xml,hdfs-default.xml
3. 	自定义名称 使用： conf.addResource("xxxx.xml");// 指定加载那个配置文件



## 网站访问量前五名-实践4


### 分析实现

1.	2个MapReduce
2.	第一个mapreduce（com.mapreduce.page.countsort.PageCountSortStep1）：完成数据的汇聚，排序。
	第二个mapreduce（com.mapreduce.page.countsort.PageCountSortStep2），完成数据的比较，输出排序


## 流量统计按照归属地输出-实践5


### 分析

1.	原始的分发规则不适用当前的分发策略
2.	修改分法策略，按照归属地分发


### 实现

1.	实现新的分发规则

```

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


```

2.	设置对应的reduce Task数量和分发规则

```
	// 设置分发规则使用类，默认是使用的HashPartitioner
	job.setPartitionerClass(ProvicePartitioner.class);
	
	// 因为分发规则的确定，所以必须制定需要几个mapreduce处理数据
	job.setNumReduceTasks(10);
```


## 倒排索引创建-实践6


### 分析

---
1、先写一个mr程序：统计出每个单词在每个文件中的总次数
（单词-文件名作为key）
hello-a.txt 4
hello-b.txt 4
hello-c.txt 4
java-c.txt 1
jerry-b.txt 1
jerry-c.txt 1

2、然后在写一个mr程序，读取上述结果数据：
map： 根据-切，以单词做key，后面一段作为value
reduce： 拼接values里面的每一段，以单词做key，拼接结果做value，输出即可

---

### 实现

com.mapreduce.index.IndexStepOne
com.mapreduce.index.IndexStepTwo


## 订单中成交金额最大的三笔-实践6


### 分析

---

实现思路：
map： 读取数据切分字段，封装数据到一个bean中作为key传输，key要按照成交金额比大小

reduce：利用自定义GroupingComparator将数据按订单id进行分组，然后在reduce方法中输出每组数据的前N条即可

---


### 实现

com.mapreduce.order.topn.OrderTopn



## 订单中成交金额最大的三笔-实践7（高效求分组）（本案例用了 排序控制、分区控制和分组控制）



### 分析

---
1.修改key的bean对象，把bean作为一个key，使得o1,800	  o1,700  o1,600 这几组key为一组，并在map中已经排序。 
2.重写groupingComparator方法 ，使得其认为订单号相同就是一组。（key的分组逻辑）
3.重写partitioner分发规则，保证id相同的数据发送给同一个reduce。
---

---
 1.首先保证一点：（分发规则：分发给reduce ）
 让orderId相同的数据发送给相同的rudece task，修改partitioner（只要Orderid相同就返回相同的分区号）
 
 2.排序规则
通过bean上的compareto方法实现让框架将数据按照orderid，次之参照订单总金额排序
 
 3.key规则（保证是同一组key，才可以分发给一个reduce）
 设法让reduce task 程序认为orderid相同的key（bean）属于同一组修改graoupingComparator 的逻辑
 
**注意：** 
 先是分发策略到reduce-----》再是分组策略（按照key分组）
 
---

### 实现

com.mapreduce.order.topn.OrderTopn


```
    // 高效排序，使用框架自己定义的分发和分组规则

    // 设置分发规则使用类，默认是使用的HashPartitioner
    // job.setPartitionerClass(OrderIdPartitioner.class);
    // 修改分组策略
    // job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
```


#### 分发策略  map阶段

```
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

```

#### 分组策略 reduce阶段

```
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

```

**注意：** 需要对需要分组的进行实例化，否则会报错，如下这段设计

```
    public OrderIdGroupingComparator() {
        // 注意：调用父类的构造函数实例化，否则会报错
        super(OrderBean.class, true);
    }
```



## 实践（两两之间有共同好友，以及共同好友是谁？）


### 分析

第一步切分数据形成 用户1-用户2  好友 ，例如  B-E A   B-F A  B-H A ；B-E A   C-B A  C-H A ；
  第二部将用户-好友最为共同key且对相同key的vlaue进行拼接


### 实现

第一步:实现切分
com.mapreduce.friends.CommonFriendsOne




## 实践：序列化的（sequence）输入输出



### 实现

```
com.mapreduce.index.sequence.IndexStepOne
    // 设置输出组件
    // job.setOutputFormatClass(TextOutputFormat.class); // 默认
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
 com.mapreduce.index.sequence.IndexStepTwo   
    // 默认输入组件
    // job.setInputFormatClass(TextInputFormat.class);//默认的文件输入系统
    job.setInputFormatClass(SequenceFileInputFormat.class);
    
```


## 实战 实现 join算法（select a.*,b.* from a join b on a.uid=b.uid;）


### 分析

---
map端：
不管worker读到的是什么文件，我们的map方法中是可以通过context来区分的
对于order数据，map中切字段，封装为一个joinbean，打标记：t_order
对于user数据，map中切字段，封装为一个joinbean，打标记：t_user
然后，以uid作为key，以joinbean作为value返回

reduce端：
用迭代器迭代出一组相同uid的所有数据joinbean，然后判断
如果是标记字段为t_order的，则加入一个arraylist<JoinBean>中
如果标记字段为t_user的，则放入一个Joinbean对象中
然后，遍历arraylist，对里面的每一个JoinBean填充userBean中的user数据，然后输出这个joinBean即可
---

### 实现

第一版：单机版，数据全部加载到内存
com.mapreduce.join.ReduceSideJoin

存在的问题：
	通过迭代器将数据全部加载到内存中，失去了分布式框架的优点
	过来的一组数据不知道user 和order 分别在哪里
			

第二版自己写：高效的(分区+排序+ 分组)

---
userid，OrderId 和表名
user001 
order01 user01....
order02 user01....
user02
order01 user02....
order02 user02....
---


----
实现上面这种情况：key(userid+表名)--->分区(userId:控制分区)----》排序(userid+表名)----》分组（控制分组）
	1.	userId**分组**，**排序**使user表的数据在最前面（根据表名排序）
	第一要考虑排序：要考虑userid的同时也要考虑表名（key = userid+ 表名）
		user001 的一组数据放在最前面的同时要将来自user表的数据放到最前面，后面的全是order表的
		只有放到key中才可以影响**排序**
	2.	为了防止因为可以种表名对分组的影响（分区的时候按照orderId分区）
		第二 将表名放到key中，可能导致数据**分区**也出现问题
		之前只是根据orderid分发 ，key中又有orderid又有表名。
		所以要控制partition，控制分区（key的hash值）；使其分组只按照orderid分组
第一种只是根据userid分发，此时又要根据userid，OrderId 和表名 分发

分区：按照userid分区
排序的时候还要根据表名排序

key ：userid，OrderId 和表名   分发
分组：userid相同就是一组

partition + 排序  + group 

最终实现： user的数据在前面，order的数据紧跟在后面。


把表明放到key中也可以影响排序


-----



## 实战-数据倾斜


### 思路一 局部聚合

Combiner --：将map task 的数据分组聚合后，传递给reduce 处理

#### 实现

类 ：WordCountCombiner
<p/>
类 ：JobSubmiterWindowsLocal


### 思路--打散key

将数据 拼接一个随机字符串，使其平均分配给每个reduce

#### 实现

---

**注意** 
分割时：<p/>
    // 注意使用不见的字符
    k.set(word + "\001" + random.nextInt(numReduceTasks));
    
---


打散key：SkewWordCount
<p/>
统计 数量： SkewWordCount2
























