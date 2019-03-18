package cn.suning.spark.test.es

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

object EsJob extends App{
  val conf = new SparkConf()
  conf.setAppName("hive_spark_job")
  conf.setMaster("local")
  conf.set("es.index.auto.create","true")
  conf.set("es.nodes","10.244.132.220")
  conf.set("es.port","9200")
  conf.set("es.nodes.wan.only","false")
  val session = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate()
  val df = session.sql("select * from person");
  df.printSchema();
  df.show(10);
  val esParams=Map("indexname"->"persion","timeformat"->"yyyyMMdd");
  write2ES(df,esParams);

  def write2ES(df:Dataset[_],esParams: Map[String,String])
  {
    var indexname = esParams.get("indexname").get
    if(null != esParams.get("timeformat"))
    {
      indexname += "_" + getDateFormatString(new Date(), Calendar.DATE, -1, esParams.get("timeformat").get)
    }
    indexname += "/feature"

    EsSparkSQL.saveToEs(df, indexname)
  }

  def getDateFormatString(date: Date, calendarField: Integer, num: Integer, timeFormat: String) : String =
  {
    val sdf = new SimpleDateFormat(timeFormat)
    val c = Calendar.getInstance();
    c.setTime(date)
    c.add(calendarField, num)
    sdf.format(c.getTime)
  }

}