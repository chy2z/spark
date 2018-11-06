package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkFilter
* @Description: Filter
  按条件过滤RDD数据
* @author chy
* @date 2018/5/17 13:58
*/
public class sparkFilter {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);

        /**
         * 获取偶数
         */
        JavaRDD<Integer> filterRDD = listRDD.filter(num -> num % 2 ==0);

        filterRDD.foreach(num -> System.out.println(num + " "));
    }

}
