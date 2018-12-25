package com.mapreduce.flow.sum;

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
