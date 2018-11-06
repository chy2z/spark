package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkJoin
* @Description: join 算子
  将两个PairRDD合并，并将有相同key的元素,合为一组，可以理解为Union和groupByKey的结合
* @author chy
* @date 2018/5/17 10:35
*/
public class sparkJoin {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtil.getJavaSparkContext();

        final List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "令狐冲"),
                new Tuple2<Integer, String>(3, "林平之")
        );

        final List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 98),
                new Tuple2<Integer, Integer>(3, 97)
        );

        final JavaPairRDD<Integer, String> nemesrdd = sc.parallelizePairs(names);

        final JavaPairRDD<Integer, Integer> scoresrdd = sc.parallelizePairs(scores);

        final JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = nemesrdd.join(scoresrdd);

        joinRDD.foreach(tuple -> System.out.println("学号:"+tuple._1+" 姓名:"+tuple._2._1+" 成绩:"+tuple._2._2));
    }
}
