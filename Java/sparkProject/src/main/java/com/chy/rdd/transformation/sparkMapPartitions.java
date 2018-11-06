package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkMapPartitions
* @Description: MapPartitions 算子
  将源RDD整个集合传入,然后迭代，最后生成新的一个新的RDD。
* @author chy
* @date 2018/5/17 9:20 
*/
public class sparkMapPartitions {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtil.getJavaSparkContext();
        Integer[] names = {1, 2, 3};
        List<Integer> list = Arrays.asList(names);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<String> resultRDD =listRDD.mapPartitions(iterator->{
            ArrayList<String> array = new ArrayList<>();
            while (iterator.hasNext()){
                array.add("hello " + iterator.next());
            }
            return array.iterator();
        });
        resultRDD.foreach(item->System.out.println(item));
    }

}
