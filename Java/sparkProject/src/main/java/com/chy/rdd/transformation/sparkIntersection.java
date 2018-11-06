package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkIntersection
* @Description: intersection 算子
  返回两个RDD的交集，并且去重
* @author chy
* @date 2018/5/17 14:10
*/
public class sparkIntersection {

    public static void main(String[] arg){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        final List<Integer> list1 = Arrays.asList(1, 22, 33, 4);
        final List<Integer> list2 = Arrays.asList(11, 22, 33, 44);
        final JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        final JavaRDD<Integer> rdd2 = sc.parallelize(list2);

        /**
         * 求交集并去重
         */
        JavaRDD<Integer> resultRdd= rdd1.intersection(rdd2);

        resultRdd.foreach(item->System.out.println(item));
    }

}
