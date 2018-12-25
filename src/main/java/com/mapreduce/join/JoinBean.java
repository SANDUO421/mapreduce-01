package com.mapreduce.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @author sanduo
 * @date 2018/09/21
 */
public class JoinBean implements Writable {

    private String orderId;
    private String userId;
    private String userName;
    private int userAge;
    private String userFriends;
    // 标志字段
    private String tableName;

    /**
     * @param orderId
     * @param userId
     * @param userName
     * @param userAge
     * @param userFriends
     */
    public void set(String orderId, String userId, String userName, int userAge, String userFriends, String tableName) {
        this.orderId = orderId;
        this.userId = userId;
        this.userName = userName;
        this.userAge = userAge;
        this.userFriends = userFriends;
        this.tableName = tableName;
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
     * @return the userName
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @param userName the userName to set
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * @return the userAge
     */
    public int getUserAge() {
        return userAge;
    }

    /**
     * @param userAge the userAge to set
     */
    public void setUserAge(int userAge) {
        this.userAge = userAge;
    }

    /**
     * @return the userFriends
     */
    public String getUserFriends() {
        return userFriends;
    }

    /**
     * @param userFriends the userFriends to set
     */
    public void setUserFriends(String userFriends) {
        this.userFriends = userFriends;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "orderId=" + orderId + ", userId=" + userId + ", userName=" + userName + ", userAge=" + userAge
            + ", userFriends=" + userFriends;
    }

    /* 序列化
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.userId);
        out.writeUTF(this.userName);
        out.writeInt(this.userAge);
        out.writeUTF(this.userFriends);
        out.writeUTF(this.tableName);
    }

    /* 反序列化
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.userId = in.readUTF();
        this.userName = in.readUTF();
        this.userAge = in.readInt();
        this.userFriends = in.readUTF();
        this.tableName = in.readUTF();

    }

}
