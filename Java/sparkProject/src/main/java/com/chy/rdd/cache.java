package com.chy.rdd;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

/**
* @Title: cache
* @Description: spark 缓存
* @author chy
* @date 2018/5/16 13:52
*/
public class cache {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> distData = sc.parallelize(data);

        /**
         * Transformation 算子
         */
        JavaRDD<Integer> rdd = distData.map((s) ->{
            System.out.println(s+":"+s);
            return  s+1;
        });

        /**
         * 使用缓存和不使用缓存的区别: 使用缓存时 distData.map 只会执行一遍，
         * 不使用缓存 distData.map 会执行2遍
         * 原因：每执行一遍count，Transformation都会执行一遍
         */

        /**
         * cache只有一个默认的缓存级别MEMORY_ONLY
         */
        //rdd.cache();

        /**
         * 而persist可以根据情况设置其它的缓存级别
         */
        rdd.persist(StorageLevel.MEMORY_AND_DISK());

        /**
         * Action 算子
         */
        long count1= rdd.count();

        /**
         * Action 算子
         */
        long count2= rdd.count();
    }
}
