package com.mapreduce.order.topn;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

/**
 * 测试序列化和对象覆盖问题
 * 
 * @author sanduo
 * @date 2018/09/13
 */
public class OrderTest {

    public static void main(String[] args) throws FileNotFoundException, IOException {

        ArrayList<OrderBean> list = new ArrayList<OrderBean>();

        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("d:/keng.txt", true));

        OrderBean bean = new OrderBean();

        bean.setOrderBean("1", "1", "1", 1, 1);
        list.add(bean);
        oos.writeObject(bean);

        bean.setOrderBean("2", "2", "2", 2, 2);
        list.add(bean);
        oos.writeObject(bean);

        bean.setOrderBean("3", "3", "3", 3, 3);
        list.add(bean);// 这个会覆盖值
        oos.writeObject(bean);// 序列化到文件中，不会覆盖值

        // 后面会把前面的覆盖掉；所以会有三个一样的值。
        System.out.println(list);
        oos.close();

    }

}
