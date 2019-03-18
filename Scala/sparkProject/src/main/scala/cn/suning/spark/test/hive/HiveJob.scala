package cn.suning.spark.test.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HiveJob extends App{
  val conf = new SparkConf()
  conf.setAppName("hive_spark_job")
  conf.setMaster("local")
  val session = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate()
  val df = session.sql("select * from person");
  df.printSchema();
  df.show(10);
}
