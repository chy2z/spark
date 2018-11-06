package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkMap
* @Description: map 算子
  将源RDD中的每个元素，经过算法转换后，生成新的一个新的RDD
* @author chy
* @date 2018/5/16 16:53
*/
public class sparkMap {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        String[] names = {"张无忌","赵敏","周芷若"};
        List<String> list = Arrays.asList(names);
        JavaRDD<String> listRDD = sc.parallelize(list);
        JavaRDD<String> nameRDD = listRDD.map(name -> {
            return "Hello " + name;
        });
        nameRDD.foreach(name -> System.out.println(name));
    }

}
