package com.chy.streaming.JavaKafkaManager;

import com.chy.util.SparkUtil;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/** 直接模式
 *  每隔batch size时间抓取数据
 *
 *  读取kafka中的数据之前，设置offset
 *  读取kafka中的数据之后,自己更新offset到zookeeper，自己管理offset
 */
public class KafkaStreamDirect_UpdateZKOffset {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws SparkException {
        String brokers="localhost:9092";
        String topics = "spark_topic";
        String groupid = "spark_streaming_group";

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id", groupid);
        kafkaParams.put("auto.offset.reset", "smallest");

        //滚动窗口
        JavaStreamingContext jssc= SparkUtil.getJavaStreamingContext(10000);

        JavaKafkaManager javaKafkaManager=new JavaKafkaManager(kafkaParams);

        JavaInputDStream<String> messages=javaKafkaManager.createDirectStream(jssc,kafkaParams,topicsSet);

        messages.foreachRDD((rdd)->{
            //OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            //更新offset
            javaKafkaManager.updateZKOffsets(rdd);
        });

        //分词
        JavaDStream<String> words = messages.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        //计数
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print(100);

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutdown hook run!");
                jssc.stop(true,true);
            }
        });

    }
}
