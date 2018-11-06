package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkMapPartitionsWithIndex
* @Description: mapPartitionsWithIndex 算子
  每次获取和处理的就是一个分区的数据,并且知道处理的分区的分区号
* @author chy
* @date 2018/5/17 9:43
*/
public class sparkMapPartitionsWithIndex {

  public static void main(String[] args) {
    JavaSparkContext sc = SparkUtil.getJavaSparkContext();
    Integer[] names = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    List<Integer> list = Arrays.asList(names);
    JavaRDD<Integer> listRDD = sc.parallelize(list, 2);

    JavaRDD<String> resultRDD = listRDD.mapPartitionsWithIndex((index, iterator) -> {
      ArrayList<String> array = new ArrayList<>();
      while (iterator.hasNext()) {
        array.add(index + "-" + iterator.next());
      }
      return array.iterator();
    }, true);

    resultRDD.foreach(item -> System.out.println(item));
  }
}
