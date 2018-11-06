package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkCoalesce
* @Description:  Coalesce 算子
  变更分区数，分区数有多变少
* @author chy
* @date 2018/5/17 14:16
*/
public class sparkCoalesce {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        List<String> list = Arrays.asList("1","2","3");

        /**
         * 3个分区
         */
        JavaRDD<String> listRDD = sc.parallelize(list,3);

        /**
         * coalesce 减少到2分区
         * mapPartitionsWithIndex 显示分区号
         */
        JavaRDD<String> resultRDD = listRDD.coalesce(2).mapPartitionsWithIndex((index, iterator) -> {
            ArrayList<String> array = new ArrayList<>();
            while (iterator.hasNext()) {
                array.add(index + "-" + iterator.next());
            }
            return array.iterator();
        }, true);

        resultRDD.foreach(item->System.out.println(item));
    }


}
