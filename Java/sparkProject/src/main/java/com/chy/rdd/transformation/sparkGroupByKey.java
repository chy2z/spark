package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
* @Title: sparkGroupByKey
* @Description: GroupByKey
  将PairRDD中拥有相同key值得元素归为一组
* @author chy
* @date 2018/5/17 10:36 
*/
public class sparkGroupByKey {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        List<Tuple2<String,String>> list = Arrays.asList(
                new Tuple2("武当", "张三丰"),
                new Tuple2("峨眉", "灭绝师太"),
                new Tuple2("武当", "宋青书"),
                new Tuple2("峨眉", "周芷若")
        );
        JavaPairRDD<String, String> listRDD = sc.parallelizePairs(list);

        /**
         * 合并key相同，合并后值是个集合
         */
        JavaPairRDD<String, Iterable<String>> groupByKeyRDD = listRDD.groupByKey();

        groupByKeyRDD.foreach(tuple -> {
            String menpai = tuple._1;
            Iterator<String> iterator = tuple._2.iterator();
            String people = "";
            while (iterator.hasNext()){
                people = people + iterator.next()+" ";
            }
            System.out.println("门派:"+menpai + "人员:"+people);
        });
    }


}
