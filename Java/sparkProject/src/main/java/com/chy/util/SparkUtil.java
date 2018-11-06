package com.chy.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
* @Title: SparkUtil
* @Description: spark 工具类
* @author chy
* @date 2018/5/16 17:20
*/
public class SparkUtil {

    /**
     * 获取 JavaSparkContext
     * @return
     */
    public static JavaSparkContext getJavaSparkContext() {
        SparkConf conf = new SparkConf().setAppName("JavaSpark").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

    /**
     * SparkSession
     * 支持数据源：textFile,load,csv,json,text,format,jdbc
     * @return
     */
    public static SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .appName("SparkSQL")
                .master("local[*]")
                .getOrCreate();
    }

}
