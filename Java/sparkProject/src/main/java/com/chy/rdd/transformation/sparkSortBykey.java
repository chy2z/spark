package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkSortBykey
* @Description:  SortBykey
  对Key-Value形式的RDD中的Key进行排序
* @author chy
* @date 2018/5/17 15:39
*/
public class sparkSortBykey {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<>(99, "张三丰"),
                new Tuple2<>(96, "东方不败"),
                new Tuple2<>(66, "林平之"),
                new Tuple2<>(98, "聂风")
        );

        JavaPairRDD<Integer, String> listRDD = sc.parallelizePairs(list,2);

        JavaPairRDD<Integer, String> resultRDD= listRDD.sortByKey(true);

        /**
         * 这样写排序输出不正确，输出的各个分区的排序结果。除非是1个分区
         */
        //resultRDD.foreach(tuple ->System.out.println(tuple._1+":"+tuple._2));

        /**
         * 这样排序结果才正确
         */
        List<Tuple2<Integer, String>> list2= resultRDD.collect();

        list2.forEach(l->{
            System.out.println(l._1()+":"+l._2());
        });
    }

}
