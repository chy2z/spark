package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkCartesian
* @Description: cartesian 算子
  笛卡尔积
* @author chy
* @date 2018/5/17 11:11
*/
public class sparkCartesian {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        List<String> list1 = Arrays.asList("A", "B");
        List<Integer> list2 = Arrays.asList(1, 2, 3);
        JavaRDD<String> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.cartesian(list2RDD).foreach(tuple -> System.out.println(tuple._1 + "->" + tuple._2));
    }

}
