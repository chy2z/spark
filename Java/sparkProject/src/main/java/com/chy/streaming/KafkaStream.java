package com.chy.streaming;

import com.chy.util.SparkUtil;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
* @Title: KafkaStream
* @Description: 每10秒采集kafka信息，重新进行计算,计算结果不包含上一次计算结果
kafka发送：
11
12
11
19
spark 计算:
(19,1)
(11,2)
(12,1)
* @author chy
* @date 2018/11/21 0:36
*/
public class KafkaStream {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        String zk = "localhost:2181";
        String groupid = "spark_streaming_group";
        String topics = "spark_topic";

        int numThreads = 1;
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topicArry = topics.split(",");
        for (String topic : topicArry) {
            topicMap.put(topic, numThreads);
        }

        JavaStreamingContext jssc=SparkUtil.getJavaStreamingContext(10000);

        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,zk, groupid, topicMap);


        JavaDStream<String> lines = kafkaStream.map(Tuple2::_2);

        lines.print();

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

    }

}
