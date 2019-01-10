package com.chy.streaming;

import com.chy.util.SparkUtil;
import org.apache.spark.streaming.Duration;
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
* @Title: KafkaStreamAndWindow
* @Description: 滑动窗口，每10秒滑动计算前20秒的信息，仅仅计算20秒内的数据
* @author chy
* @date 2018/11/21 23:30
*/
public class KafkaStreamAndWindow {

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

        JavaStreamingContext jssc= SparkUtil.getJavaStreamingContext(10000);

        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(jssc,zk, groupid, topicMap);


        JavaDStream<String> lines = kafkaStream.map(Tuple2::_2);

        lines.print();

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        words.print();

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                //每隔10秒(slideInterval),统计前20秒(windowLength)
                //slideInterval 不小于 SparkUtil 读取流的间隔时间
                .reduceByKeyAndWindow((i1, i2) -> i1 + i2,new Duration(20000),new Duration(10000));

        wordCounts.print();

        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
