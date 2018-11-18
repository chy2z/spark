package com.chy.rdd.hdfs;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by chy on 2018/7/27.
 */
public class readHdfs {

    public static void main(String[] args) {

        JavaSparkContext sc= SparkUtil.getJavaSparkContext();

        /**
         * hive 数据
         */
        JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/user/hive/warehouse/kvsrc/kv1.txt");

        JavaRDD<String> tempRDD = lines.flatMap(line-> Arrays.asList(line.split("\u0001")).iterator());

        JavaRDD<String> nameRDD = tempRDD.map(name -> {
            return name;
        });

        nameRDD.foreach(name -> System.out.println(name));


    }

    public void test1(){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();

        /**
         * 文件路径
         */
        JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/chy-data/hdfs2.txt");

        JavaRDD<String> tempRDD = lines.flatMap(line-> Arrays.asList(line.split(",")).iterator());

        JavaRDD<String> nameRDD = tempRDD.map(name -> {
            return  name;
        });

        nameRDD.foreach(name -> System.out.println(name));
    }
}
