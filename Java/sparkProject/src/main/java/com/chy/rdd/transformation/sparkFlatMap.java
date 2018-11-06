package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkFlatMap
* @Description: FlatMap 算子
  将源RDD中的每个元素，经过算法转换后,每一个输入元素可以被映射为0或多个输出元素,生成新的一个新的RDD
* @author chy
* @date 2018/5/16 17:17
*/
public class sparkFlatMap {

  public static void main(String[] args){
    JavaSparkContext sc= SparkUtil.getJavaSparkContext();
    String[] names = {"李元霸,张无忌","赵敏","周芷若,周星驰"};
    List<String> list = Arrays.asList(names);
    JavaRDD<String> listRDD = sc.parallelize(list);
    JavaRDD<String> tempRDD = listRDD.flatMap(line->Arrays.asList(line.split(",")).iterator());
    JavaRDD<String> nameRDD = tempRDD.map(name -> {
      return "Hello " + name;
    });
    nameRDD.foreach(name -> System.out.println(name));
  }
}
