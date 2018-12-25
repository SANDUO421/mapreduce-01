package com.mapreduce.maxvisit;

import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * 测试TreeMap
 * 
 * @author sanduo
 * @date 2018/09/07
 */
public class TreeMapTest {
    public static void main(String[] args) {

        // test01();
        test02();
    }

    /**
     * 比较总流量-使用比较器
     * 
     * 注意：当compare比较的结果为0 ，就会跳过
     */
    private static void test02() {
        // 传递构造器比较
        /*TreeMap<FlowBean, String> tm = new TreeMap<FlowBean, String>(new Comparator<FlowBean>() {
        
            public int compare(FlowBean o1, FlowBean o2) {
                // 倒序
                return o2.getAmountFlow() - o1.getAmountFlow();
        
            };
        });*/
        // 类型中实现比较器比较
        TreeMap<FlowBean, String> tm = new TreeMap<FlowBean, String>();

        tm.put(new FlowBean("135888", 500, 300), null);
        tm.put(new FlowBean("135777", 400, 200), null);
        tm.put(new FlowBean("135666", 600, 400), null);
        tm.put(new FlowBean("135555", 300, 500), null);
        tm.put(new FlowBean("135444", 200, 400), null);

        for (Entry<FlowBean, String> entry : tm.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

    }

    /**
     * 
     */
    @SuppressWarnings("unused")
    private static void test01() {
        TreeMap<String, Integer> tm = new TreeMap<String, Integer>();
        tm.put("aa", 2);
        tm.put("ab", 1);
        tm.put("ba", 3);
        tm.put("b", 4);

        for (Entry<String, Integer> map : tm.entrySet()) {
            System.out.println(map.getKey() + ":" + map.getValue());

        }
    }

}
