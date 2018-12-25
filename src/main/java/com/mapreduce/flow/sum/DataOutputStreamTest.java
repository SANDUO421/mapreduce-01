package com.mapreduce.flow.sum;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 把数据字节流的形式写入文件
 * 
 * @author sanduo
 * @date 2018/09/06
 */
public class DataOutputStreamTest {
    public static void main(String[] args) throws IOException {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream("d:/a.txt"));
        dos.write("我爱你".getBytes());//
        dos.close();

        DataOutputStream dos2 = new DataOutputStream(new FileOutputStream("d:/b.txt"));
        dos2.writeUTF("我爱你");//
        dos2.close();

    }

}
