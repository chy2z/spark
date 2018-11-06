package com.chy.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* @Title: initSpark
* @Description: 初始化spark
* @author chy
* @date 2018/5/16 9:51
*/
public class initSpark {


    public static void main(String[] args){

        /**
         local 本地单线程
         local[K] 本地多线程（指定K个内核）
         local[*] 本地多线程（指定所有可用内核）
         spark://HOST:PORT  连接到指定的 Spark standalone cluster master，需要指定端口。
         mesos://HOST:PORT  连接到指定的  Mesos 集群，需要指定端口。
         yarn-client客户端模式 连接到 YARN 集群。需要配置 HADOOP_CONF_DIR。
         yarn-cluster集群模式 连接到 YARN 集群
         。需要配置 HADOOP_CONF_DIR。
         */
        //SparkConf conf = new SparkConf().setAppName("JavaSpark").setMaster("local[2]");
        SparkConf conf = new SparkConf().setAppName("JavaSpark").setMaster("local[*]");

        //没测试通过
        //SparkConf conf = new SparkConf().setAppName("Spark shell")
        //      .setMaster("spark://localhost:4040").set("spark.ui.port","4040");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> distData = sc.parallelize(data);

        distData.foreach((x) ->{
            System.out.println(x);
        });


    }
}
