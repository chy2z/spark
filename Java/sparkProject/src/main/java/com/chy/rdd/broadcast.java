package com.chy.rdd;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
* @Title: broadcast
* @Description: 广播
* @author chy
* @date 2018/5/16 15:51
*/
public class broadcast {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();

        Broadcast<int[]> bArry=sc.broadcast(new int[] {1, 2, 3});

        Broadcast<String> bStr=sc.broadcast("chy");

        for (int i : bArry.getValue()) {
            System.out.println(i);
        }

        System.out.println(bStr.getValue());
    }

}
