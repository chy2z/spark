package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkRepartition
* @Description: Replication 重新分区
  变更分区数，分区数有少变多
* @author chy
* @date 2018/5/17 14:25
*/
public class sparkRepartition {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        List<String> list = Arrays.asList("1","2","3","4");

        /**
         * 1个分区
         */
        JavaRDD<String> listRDD = sc.parallelize(list,1);

        /**
         * repartition 增加到2分区
         * mapPartitionsWithIndex 显示分区号
         */
        JavaRDD<String> resultRDD = listRDD.repartition(2).mapPartitionsWithIndex((index, iterator) -> {
            ArrayList<String> array = new ArrayList<>();
            while (iterator.hasNext()) {
                array.add(index + "-" + iterator.next());
            }
            return array.iterator();
        }, true);

        resultRDD.foreach(item->System.out.println(item));
    }

}
