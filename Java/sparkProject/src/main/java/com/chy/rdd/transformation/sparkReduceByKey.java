package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkReduceByKey
* @Description: reduceByKey 算子
  将RDD中所有K,V对中,K值相同的V,进行合并
* @author chy
* @date 2018/5/17 10:19
*/
public class sparkReduceByKey {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();

        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("武当", 1),
                new Tuple2<String, Integer>("少林", 2),
                new Tuple2<String, Integer>("武当", 4),
                new Tuple2<String, Integer>("少林", 3)
        );

        JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);

        JavaPairRDD<String, Integer> resultRDD=listRDD.reduceByKey((x,y)->x+y);

        resultRDD.foreach(name -> System.out.println(name));
    }

}
