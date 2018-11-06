package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkUnion
* @Description: Union 算子
  简单的将两个源RDD合并在一起，返回新的RDD,必须保证两个RDD的泛型是一致的
* @author chy
* @date 2018/5/17 10:27
*/
public class sparkUnion {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        final List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        final List<Integer> list2 = Arrays.asList(11, 22, 33, 44);
        final JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        final JavaRDD<Integer> rdd2 = sc.parallelize(list2);
        JavaRDD<Integer> resultRdd=rdd1.union(rdd2);
        resultRdd.foreach(x->System.out.println(x));
    }

}
