package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkDistinct
* @Description:  Distinct 算子
  去重复
* @author chy
* @date 2018/5/17 14:05
*/
public class sparkDistinct {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 3, 2, 1);
        JavaRDD<Integer> listRDD = sc.parallelize(list);

        /**
         * 去重复
         */
        JavaRDD<Integer> filterRDD = listRDD.distinct();

        filterRDD.foreach(num -> System.out.println(num + " "));
    }

}
