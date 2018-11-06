package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkRepartitionAndSortWithinPartitions
* @Description:  RepartitionAndSortWithinPartitions
   在给定的各个分区内部进行排序，性能比repartition要高
* @author chy
* @date 2018/5/17 14:38
*/
public class sparkRepartitionAndSortWithinPartitions {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtil.getJavaSparkContext();
        List<Integer> list = Arrays.asList(1, 4, 55, 77,22,66, 33, 48, 23);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 1);

        /**
         * 转换成 k,v 形式
         */
        JavaPairRDD<Integer, Integer> pairRDD = listRDD.mapToPair(num -> new Tuple2<>(num, num));

        /**
         * 增加分区
         */
        pairRDD = pairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(2));

        JavaRDD<String> resultRDD= pairRDD.mapPartitionsWithIndex((index, iterator) -> {
                    ArrayList<String> list1 = new ArrayList<>();
                    while (iterator.hasNext()) {
                        list1.add(index + "_" + iterator.next());
                    }
                    return list1.iterator();
                }, false);

        resultRDD.foreach(item->System.out.println(item));
    }

}
