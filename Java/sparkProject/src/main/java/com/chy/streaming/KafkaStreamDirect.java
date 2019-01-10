package com.chy.streaming;


import com.chy.util.SparkUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/** 直接模式
 * 并不需要单独线程实时接收数据，而是每隔batch size时间抓取数据
 * 读取kafka中的数据之后不需要更新offset到zookeeper而是spark自己管理，把offset存储到内存中，
 * 如果设置了checkpoint那么offset会保存到文件中一份保存在内存一份
 * 不需要设置WAL，减少了存储数据到hdfs的步骤增加了job的执行速度
 */
public class KafkaStreamDirect {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        String brokers="localhost:9092";
        String topics = "spark_topic";
        String groupid = "spark_streaming_group";

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id", groupid);
        kafkaParams.put("auto.offset.reset", "smallest");
        //kafkaParams.put("auto.offset.reset", "largest");


        JavaStreamingContext jssc= SparkUtil.getJavaStreamingContext(10000);

        jssc.checkpoint("src/main/resources/checkpoint");

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        words.print();

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();

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
