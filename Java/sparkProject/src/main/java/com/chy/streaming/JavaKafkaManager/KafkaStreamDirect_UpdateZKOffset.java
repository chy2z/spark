package com.chy.streaming.JavaKafkaManager;

import com.chy.util.SparkUtil;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
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


        JavaStreamingContext jssc= SparkUtil.getJavaStreamingContext(10000);


        JavaKafkaManager javaKafkaManager=new JavaKafkaManager(kafkaParams);


        JavaInputDStream<String> messages=javaKafkaManager.createDirectStream(jssc,kafkaParams,topicsSet);


        //分词
        JavaDStream<String> words = messages.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        //更新zk
        words.foreachRDD(new VoidFunction<JavaRDD<String>>(){
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                System.out.println(rdd);
                if (!rdd.isEmpty()){
                    rdd.foreach(new VoidFunction<String>() {
                        @Override
                        public void call(String r) throws Exception {
                            System.out.println(r);
                        }
                    });
                    javaKafkaManager.updateZKOffsets(rdd);
                }
            }
        });


        words.print(100);

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
