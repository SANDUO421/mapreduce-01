package com.mapreduce.order.topn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

/**
 * 订单详情
 * 
 * @author sanduo
 * @date 2018/09/13
 */
/**
 * @author sanduo
 * @date 2018/09/13
 */
public class OrderBean implements WritableComparable<OrderBean>, Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    // 订单id
    private String orderId;
    // 用户id
    private String userId;
    // 商品名称
    private String pdtName;
    // 商品价格
    private float price;
    // 商品数量
    private int num;
    // 订单总价
    private float amountfree;

    /**
     * @param orderId
     * @param userId
     * @param pdtName
     * @param price
     * @param num
     * @param amountfree
     */
    public void setOrderBean(String orderId, String userId, String pdtName, float price, int num) {
        this.orderId = orderId;
        this.userId = userId;
        this.pdtName = pdtName;
        this.price = price;
        this.num = num;
        this.amountfree = price * num;
    }

    /**
     * @return the orderId
     */
    public String getOrderId() {
        return orderId;
    }

    /**
     * @param orderId the orderId to set
     */
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    /**
     * @return the userId
     */
    public String getUserId() {
        return userId;
    }

    /**
     * @param userId the userId to set
     */
    public void setUserId(String userId) {
        this.userId = userId;
    }

    /**
     * @return the pdtName
     */
    public String getPdtName() {
        return pdtName;
    }

    /**
     * @param pdtName the pdtName to set
     */
    public void setPdtName(String pdtName) {
        this.pdtName = pdtName;
    }

    /**
     * @return the price
     */
    public float getPrice() {
        return price;
    }

    /**
     * @param price the price to set
     */
    public void setPrice(float price) {
        this.price = price;
    }

    /**
     * @return the num
     */
    public int getNum() {
        return num;
    }

    /**
     * @param num the num to set
     */
    public void setNum(int num) {
        this.num = num;
    }

    /**
     * @return the amountfree
     */
    public float getAmountfree() {
        return amountfree;
    }

    /**
     * @param amountfree the amountfree to set
     */
    public void setAmountfree(float amountfree) {
        this.amountfree = amountfree;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "orderId=" + orderId + ", userId=" + userId + ", pdtName=" + pdtName + ", price=" + price + ", num="
            + num + ", amountfree=" + amountfree;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(userId);
        out.writeUTF(this.pdtName);
        out.writeFloat(price);
        out.writeInt(num);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        userId = in.readUTF();
        pdtName = in.readUTF();
        price = in.readFloat();
        num = in.readInt();
        amountfree = price * num;
    }

    /* 排序
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(OrderBean o) {
        // 比较规则：先比较订单总价（倒序），在比较订单名称（升序）
        return Float.compare(o.getAmountfree(), this.getAmountfree()) == 0 ? this.pdtName.compareTo(o.pdtName)
            : Float.compare(o.getAmountfree(), this.getAmountfree());
    }

}
