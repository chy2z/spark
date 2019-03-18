package cn.suning.spark.etl.job.imp

import java.util.{Calendar, Date}

import cn.suning.spark.etl.job.SparkJob
import com.alibaba.fastjson.JSON
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkException
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.HashMap


class Kafka2ESDataLoadJob(args : Array[String]) extends SparkJob {
  conf.setAppName("tusf_sparkstreaming_job")
  conf.set("spark.streaming.backpressure.enabled","false")
  conf.set("spark.streaming.backpressure.initialRate","200")
  conf.set("spark.streaming.kafka.maxRatePerPartition","100")
  conf.set("spark.streaming.stopGracefullyOnShutdown","true")
  conf.set("es.index.auto.create","true")
  val esNodes = getEsNodes(args(0).split(";#")(1)) 
  if(null != esNodes && !"".equals(esNodes)) {
    conf.set("es.nodes",esNodes)
  }
  conf.set("es.port","9200")
  conf.set("es.nodes.wan.only","false")
  val sparkConfMap = parseArg(args(0).split(";#")(2))
  val interval = sparkConfMap.getOrElse("interval", "15").toLong
  sparkConfMap.remove("interval")
  initSparkConf(sparkConfMap)
  val ssc = new StreamingContext(conf, Seconds(interval))
  def exe = {
    val kafkaParams = new HashMap[String,String]()
    //kafkaParams.put("bootstrap.servers","10.242.142.155:9092,10.242.142.156:9092,10.242.142.157:9092")
    kafkaParams.put("offsets.storage","kafka")
    kafkaParams.put("dual.commit.enabled","true")
    kafkaParams.put("fetch.message.max.bytes","10485760")
    parseArg(args(0).split(";#")(3)).foreach(kafkaParams.+=_)
    val esParams = parseArg(args(0).split(";#")(4))
    
    val tableInfoArray = getTableInfoFromHttpAddress(args(0).split(";#")(1))
    tableInfoArray.append((kafkaParams.toMap,esParams.toMap))
    println(tableInfoArray)
    tableInfoArray.foreach(f => sparkStreamingJob(f._1,f._2))
    ssc.start()
	  //等待计算终止
    ssc.awaitTermination()
    //true    会把内部的sparkcontext同时停止
    //false  只会停止streamingcontext  不会停sparkcontext
    //ssc.stop(true)
  }
  
  def sparkStreamingJob(kafkaParams : Map[String, String], esParams : Map[String, String]) =
  {
    val kafkaCluster = new KafkaCluster(kafkaParams)
    setOrUpdateOffsets(kafkaParams.get("topics").toSet,kafkaParams.get("group.id").get,kafkaCluster)
    
    val topicAndPartitionSet = kafkaCluster.getPartitions(kafkaParams.get("topics").toSet).right.get
    val fromOffset = new scala.collection.mutable.HashMap[TopicAndPartition, Long]()
    val consumerOffsetsTemp = kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id").get, topicAndPartitionSet)
      topicAndPartitionSet.foreach(tp => {
        fromOffset.put(tp, consumerOffsetsTemp.right.get(tp))
      })
    
    val kafkaClusterParamsBroadcast = ssc.sparkContext.broadcast(kafkaParams)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
                              ssc, 
                              kafkaParams, 
                              fromOffset.toMap, 
                              (mmd: MessageAndMetadata[String, String]) => mmd.message()
                            )
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val indexNamePrefix = esParams.get("indexname").get
      val timeString = getDateFormatString(new Date(), Calendar.DATE, 0, esParams.get("timeformat").get)
      val esResource = indexNamePrefix+ "_" + timeString + "/feature"
      val esRdd = rdd.map(f => {
          JSON.parseObject(f)
      })
      EsSpark.saveToEs(esRdd, esResource)

      val readOffset = new scala.collection.mutable.HashMap[TopicAndPartition, Long]()
      if (null != offsetRanges) {
        offsetRanges.foreach(
          osr => {
            val tp = osr.topicAndPartition
            readOffset.put(tp, osr.untilOffset)
          }
        )
      }
      
      kafkaCluster.setConsumerOffsets(kafkaClusterParamsBroadcast.value.get("group.id").get, readOffset.toMap)
    }
  }
  
  def setOrUpdateOffsets(implicit topics: Set[String], groupId: String, kc: KafkaCluster): Unit = {
    topics.foreach(topic => {
      println("current topic:" + topic)
      println("groupId:" + groupId)
      var hasConsumed = true
      val kafkaPartitionsE = kc.getPartitions(topics)
      if (kafkaPartitionsE.isLeft) throw new SparkException("get kafka partition failed:")
      val kafkaPartitions = kafkaPartitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, kafkaPartitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) {
        //如果有消费过，有两种可能，如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
        //针对这种情况，只要判断一下zk上的consumerOffsets和leaderEarliestOffsets的大小，如果consumerOffsets比leaderEarliestOffsets还小的话，说明是过时的offsets,这时把leaderEarliestOffsets更新为consumerOffsets
        val leaderEarliestOffsets = kc.getEarliestLeaderOffsets(kafkaPartitions).right.get
        val consumerOffsets = consumerOffsetsE.right.get
        //println("leaderEarliestOffsets:" + leaderEarliestOffsets)
        //println("consumerOffsets:" + consumerOffsets)
        val isNormal = consumerOffsets.forall {
          case (tp, n) => n > leaderEarliestOffsets(tp).offset
        }
        if (!isNormal) {
          println("consumed Offsets of some partition is out of time, update to leaderEarliestOffsets")
          val offsets = consumerOffsets.map {
            case (tp, offset) => {
              val leaderEarliestOffset = leaderEarliestOffsets.get(tp).get.offset
              if(offset < leaderEarliestOffset) {
                println("update partion (" + tp + ") offset : " + offset + "->" + leaderEarliestOffset)
                (tp, leaderEarliestOffset)
              }else
              {
                (tp, offset)
              }
            }
          }
          kc.setConsumerOffsets(groupId, offsets)
        }
      }
      else {
        //如果没有被消费过，则从最新的offset开始消费。
        val leaderLatestOffsets = kc.getLatestLeaderOffsets(kafkaPartitions).right.get
        println("has not Consumed,leaderLatestOffsets:" + leaderLatestOffsets)
        println("consumer group:" + groupId + " has not Consumed，update to leaderLatestOffsets")
        val offsets = leaderLatestOffsets.map {
          case (tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(groupId, offsets)
      }    
    })
  }
}