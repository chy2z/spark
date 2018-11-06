package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

/**
* @Title: sparkSample
* @Description: Sample 算子
  用来从RDD中抽取样本。他有三个参数
* @author chy
* @date 2018/5/17 11:00
*/
public class sparkSample {

    public static void main(String[] args){

        JavaSparkContext sc= SparkUtil.getJavaSparkContext();

        ArrayList<Integer> list = new ArrayList<>();

        for(int i=1;i<=100;i++){
            list.add(i);
        }

        JavaRDD<Integer> listRDD = sc.parallelize(list);

        /**
         * sample用来从RDD中抽取样本。他有三个参数
         * withReplacement: Boolean,
         *       true: 有放回的抽样
         *       false: 无放回抽抽样
         * fraction: Double：
         *      抽取样本的比例
         * seed: Long：
         *      随机种子
         */
        JavaRDD<Integer> sampleRDD = listRDD.sample(false, 0.1, 0);

        sampleRDD.cache();

        sampleRDD.foreach(num -> System.out.println(num + " "));

        System.out.println("总计:"+sampleRDD.count());
    }

}
