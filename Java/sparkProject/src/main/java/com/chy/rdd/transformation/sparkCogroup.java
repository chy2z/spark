package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkCogroup
* @Description:  Cogroup 算子
  对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。
* @author chy
* @date 2018/5/17 15:14
*/
public class sparkCogroup {

    public static void  main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();

        List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<Integer, String>(1, "www"),
                new Tuple2<Integer, String>(2, "bbs")
        );

        List<Tuple2<Integer, String>> list2 = Arrays.asList(
                new Tuple2<Integer, String>(1, "cnblog"),
                new Tuple2<Integer, String>(2, "cnblog"),
                new Tuple2<Integer, String>(3, "very")
        );

        List<Tuple2<Integer, String>> list3 = Arrays.asList(
                new Tuple2<Integer, String>(1, "com"),
                new Tuple2<Integer, String>(2, "com"),
                new Tuple2<Integer, String>(3, "good")
        );

        JavaPairRDD<Integer, String> list1RDD = sc.parallelizePairs(list1);
        JavaPairRDD<Integer, String> list2RDD = sc.parallelizePairs(list2);
        JavaPairRDD<Integer, String> list3RDD = sc.parallelizePairs(list3);

        JavaPairRDD<Integer, Tuple3<Iterable<String>,Iterable<String>,Iterable<String>>> resultRDD =list1RDD.cogroup(list2RDD,list3RDD);

        resultRDD.foreach(tuple ->
                System.out.println(tuple._1+" " +tuple._2._1() +" "+tuple._2._2()+" "+tuple._2._3()));
    }

}
