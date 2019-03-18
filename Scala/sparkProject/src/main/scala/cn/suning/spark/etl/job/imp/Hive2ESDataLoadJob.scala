package cn.suning.spark.etl.job.imp

import java.util.{Calendar, Date}

import cn.suning.spark.etl.job.SparkJob
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

class Hive2ESDataLoadJob(args : Array[String]) extends SparkJob {
  conf.setAppName("tusf_spark_job")
  conf.set("es.index.auto.create","true")
  val test = args(0).split(";#")
  test.foreach(println)
  val esNodes = getEsNodes(args(0).split(";#")(1)) 
  if(null != esNodes && !"".equals(esNodes)) {
    conf.set("es.nodes",esNodes)
  }
  conf.set("es.port","9200")
  conf.set("es.nodes.wan.only","false")
  val confMap = parseArg(args(0).split(";#")(2))
  initSparkConf(confMap)
  val session = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate()
  
  def exe = {
    val tableMap = parseArg(args(0).split(";#")(3))
    val indexMap = parseArg(args(0).split(";#")(4))
    val tableInfoArray = getTableInfoFromHttpAddress(args(0).split(";#")(1))
    tableInfoArray.append((tableMap.toMap,indexMap.toMap))
    println(tableInfoArray)
    tableInfoArray.foreach(f => sparkJob(f._1,f._2))
    session.stop()
  }
  
  def sparkJob(tableParams: Map[String,String], esParams: Map[String,String]) = {
    var sql = "select * from "
    val tableName = tableParams.get("tablename").get
    sql += tableName
    if(tableParams.get("partitionfield").isDefined && tableParams.get("dateformat").isDefined)
    {
      val dateString = getDateFormatString(new Date(), Calendar.DATE, -1, tableParams.get("dateformat").get)
      sql += " where " + tableParams.get("partitionfield").get + " = '" + dateString + "'"
    }
    println("sql:" + sql)
    
    val pos = tableName.indexOf(".")
    if(pos > 0)
    {
      val db = tableName.substring(0, pos)
      val tb = tableName.substring(pos+1)
      println("database:" + db)
        println("tablename:" + tb)
      if(session.catalog.tableExists(db,tb))
      {
        val df = session.sql(sql)
        write2ES(df,esParams)
      }else
      {
        println("table name:" + tableName + ", table doesn't exist")
      }
    }else
    {
      println("table name:" + tableName + ",get database failed")
      if(session.catalog.tableExists(tableName))
      {
        val df = session.sql(sql)
        write2ES(df,esParams)
      }else
      {
        println("table name:" + tableName + ", table doesn't exist")
      }
    }
  }
  
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
}