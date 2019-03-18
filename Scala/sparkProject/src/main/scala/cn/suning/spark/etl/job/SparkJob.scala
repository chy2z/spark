package cn.suning.spark.etl.job

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scalaj.http.Http

trait SparkJob extends Job{
  val conf = new SparkConf()
  def initSparkConf(confMap : HashMap[String,String]) : Unit = {
    conf.setMaster("local")
    confMap.foreach(f => conf.set(f._1, String.valueOf(f._2)))
  }
  
  def parseArg(arg : String): HashMap[String,String] = {
    val confMap = new HashMap[String,String]()
    if(null != arg && !"".equals(arg))
    {
      val confArray = arg.split("#")
      confArray.foreach(f => {
        val fArray = f.split("=")
        if(fArray.length == 2)
        {
          confMap.put(fArray(0),fArray(1))
        }
      })
    }
    confMap
  }
  
  def getEsNodes(httpUrl: String) : String = {
    if(null != httpUrl && !"".equals(httpUrl))
    {
      val http = Http(httpUrl).asString
		  if (http.code != 200)
			  throw new IllegalArgumentException()
      println(http.body)
		  val paramJson = JSON.parseObject(http.body, classOf[JSONObject])
		  paramJson.getString("esClusterNodes")
    }else
    {
      null
    }
  }
  
  def getTableInfoFromHttpAddress(httpUrl: String) : ArrayBuffer[(Map[String, String], Map[String, String])] = {
    val tableInfoArray = new ArrayBuffer[(Map[String, String], Map[String, String])]()
    if(null != httpUrl && !"".equals(httpUrl))
    {
      val http = Http(httpUrl).asString
		  if (http.code != 200) throw new IllegalArgumentException("httpUrl request error")
      println(http.body)
		  val paramJson = JSON.parseObject(http.body, classOf[JSONObject])
		  val esClusterNodes = paramJson.getString("esClusterNodes")
		  val tableJsonArray = paramJson.getJSONArray("data")
		  for (i <- 0 to tableJsonArray.length-1)
		  {
		    val tableInfo = tableJsonArray.getJSONObject(i)
		    tableInfo.getString("type") match
		    {
		      case "offline" => {
		        val tableParams = Map("tablename" -> tableInfo.getString("tableName"))
		        tableParams.+("granularity" -> tableInfo.getString("granularity"))
		        tableParams.+("partitionfield" -> tableInfo.getString("partitionfield"))
		        tableInfo.getString("granularity") match {
		          case "day" => tableParams.+("dateformat" -> "yyyyMMdd")
		          case "week" => tableParams.+("dateformat" -> "yyyyMMdd")
		          case "month" => tableParams.+("dateformat" -> "yyyyMM")
		          case "year" => tableParams.+("dateformat" -> "yyyy")
		          case _ => println("unsupport granularity")
		        }
		        val esParams = Map("indexname" -> tableInfo.getString("indexName"))
		        esParams.+("granularity" -> tableInfo.getString("granularity"))
		        tableInfo.getString("granularity") match {
		          case "day" => esParams.+("timeformat" -> "yyyyMMdd")
		          case "week" => esParams.+("timeformat" -> "yyyyMMdd")
		          case "month" => esParams.+("timeformat" -> "yyyyMM")
		          case "year" => esParams.+("timeformat" -> "yyyy")
		          case _ => println("unsupport granularity")
		        }
		      
	          //tableInfoArray.append((tableParams,esParams))
		      }
		      case "realtime" => {
		        val kafkaParams = Map("topics" -> tableInfo.getString("tableName"))
		        kafkaParams.+("bootstrap.servers" -> tableInfo.getString("kafkaCluster"))
		        kafkaParams.+("group.id" -> "tusf_spark")
		        val esParams = Map("indexname" -> tableInfo.getString("indexName"))
		        esParams.+("granularity" -> tableInfo.getString("granularity"))
		        tableInfo.getString("granularity") match {
		          case "day" => esParams.+("timeformat" -> "yyyyMMdd")
		          case "week" => esParams.+("timeformat" -> "yyyyMMdd")
		          case "month" => esParams.+("timeformat" -> "yyyyMM")
		          case "year" => esParams.+("timeformat" -> "yyyy")  
		          case _ => println("unsupport granularity")
		        }
		      		      
	          //tableInfoArray.append((kafkaParams,esParams))
		      }
		      case _ => println("unsupport table type")
		    }
		  }
    }
    
    tableInfoArray
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