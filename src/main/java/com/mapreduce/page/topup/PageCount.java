package com.mapreduce.page.topup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 统计页面访问的频率
 * 
 * 如果不用传输，该类可以不用序列化
 * 
 * @author sanduo
 * @date 2018/09/07
 */
public class PageCount implements Writable, Comparable<PageCount> {

    private String page;
    private int count;

    /**
     * @param page
     * @param count
     */
    public void set(String page, int count) {
        this.page = page;
        this.count = count;
    }

    /**
     * @return the page
     */
    public String getPage() {
        return page;
    }

    /**
     * @param page the page to set
     */
    public void setPage(String page) {
        this.page = page;
    }

    /**
     * @return the count
     */
    public int getCount() {
        return count;
    }

    /**
     * @param count the count to set
     */
    public void setCount(int count) {
        this.count = count;
    }

    /* toString
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "page=" + page + ", count=" + count;
    }

    /* 反序列化
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    public void readFields(DataInput in) throws IOException {

        page = in.readUTF();
        count = in.readInt();
    }

    /* 序列化
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(page);
        out.writeInt(count);

    }

    /* 比较器
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(PageCount o) {
        // 降序
        return o.getCount() - this.count == 0 ? this.page.compareTo(o.getPage()) : o.getCount() - this.count;
    }

}
