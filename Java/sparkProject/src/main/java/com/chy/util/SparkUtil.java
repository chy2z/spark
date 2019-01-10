package com.chy.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

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
     * 获取 JavaStreamingContext
     * @return
     */
    public static JavaStreamingContext getJavaStreamingContext(int duration) {
        SparkConf conf = new SparkConf().setAppName("JavaSpark").setMaster("local[*]");
        return new JavaStreamingContext(conf, new Duration(duration));
    }

    /**
     * 获取 JavaStreamingContext
     * @return
     */
    public static JavaStreamingContext getJavaStreamingContext(int duration,boolean writeAheadLog){
        SparkConf conf = new SparkConf().setAppName("JavaSpark").setMaster("local[*]");
        if(writeAheadLog) {
            conf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
        }
        return new JavaStreamingContext(conf, new Duration(duration));
    }

    /**
     * 从检查点路径恢复或者创建新的
     * @param directory
     * @param duration
     * @return
     */
    public static JavaStreamingContext getOrCreateJavaStreamingContext(String directory,int duration,Function0<JavaStreamingContext> creatingFunc) {
        return JavaStreamingContext.getOrCreate(directory, creatingFunc);
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

    /**
     * SparkSession for hive
     * 支持数据源：hive
     * @return
     */
    public static SparkSession getSparkSessionForHive() {
        return SparkSession
                .builder()
                .appName("SparkSQLForHive")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
    }

}
