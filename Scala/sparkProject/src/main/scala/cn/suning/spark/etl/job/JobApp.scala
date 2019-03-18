package cn.suning.spark.etl.job

import cn.suning.spark.etl.job.imp.{Hive2ESDataLoadJob, Kafka2ESDataLoadJob}

/**
 * args参数说明:
 *          args(0)-任务类型##httpaddress##sparkConf配置##hive表或topic配置##ES索引名称配置
 *          任务类型：字符串，目前支持hive和kafka
 *          sparkConf配置：配置项名字:配置项#配置项名字:配置项#......
 *          hive表或topic配置： 配置项名字:配置项#配置项名字:配置项#......
 *          ES索引名称配置：配置项名字:配置项#配置项名字:配置项#......
 * 样例：Hive2ESDataLoadJob AppName:sparkbatchprocessingjob#es.index.auto.create:true#es.nodes:10.244.132.220#es.port:9200#es.nodes.wan.only:true tablename:fdm_dpa.refund_tbhj_cs_full_details_01#partitionfield:data_date#dateformat:yyyyMMdd indexname:fdm_dpa.refund_tbhj_cs_full_details_01#timeformat:yyyyMMdd#type:test
 */
object JobApp extends App{
  
  args(0).split(";#")(0) match
  {
    case "hive" => new Hive2ESDataLoadJob(args).exe
    case "kafka" => new Kafka2ESDataLoadJob(args).exe
    case _ => println("Don't Surport job type")
  }
}