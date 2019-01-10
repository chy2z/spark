package com.chy.streaming;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.storage.StorageLevel;
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
 * 基于Receiver方式实现会利用Kakfa的高层消费API
 * 在默认的配置下，这种方式在失败的情况下，会丢失数据，如果要保证零数据丢失，需要启用WAL(Write Ahead Logs)。
 * 它同步将接受到数据保存到分布式文件系统上比如HDFS。 所以数据在出错的情况下可以恢复出来。
 * 如果启用了WAL，接收到的数据会被持久化一份到日志中，因此需要将storage_lever设置成StorgeLevel.MEMORY_AND_DISK_SER
 * @Description: 滑动窗口，每10秒滑动计算前20秒的信息，增量计算，减去离开的加上新增的数据
 * @author chy
 * @date 2018/11/21 23:30
 */
public class KafkaStreamAndWindow_Two {

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

        String directory="src/main/resources/checkpoint/KafkaStreamAndWindow_Two";

        JavaStreamingContext jssc=JavaStreamingContext.getOrCreate(directory, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {

                final JavaStreamingContext jssc= SparkUtil.getJavaStreamingContext(10000,true);

                //保证元数据恢复，就是Driver端挂了之后数据仍然可以恢复
                jssc.checkpoint(directory);

                JavaPairReceiverInputDStream<String, String> kafkaStream =
                        KafkaUtils.createStream(jssc,zk, groupid, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2());


                JavaDStream<String> lines = kafkaStream.map(Tuple2::_2);

                lines.print();

                JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

                words.print();

                JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                        //每隔10秒(slideInterval),统计前20秒(windowLength)
                        //slideInterval 不小于 SparkUtil 读取流的间隔时间
                        .reduceByKeyAndWindow((i1, i2) -> i1 + i2,(i1, i2) -> i1 - i2,new Duration(20000),new Duration(10000));

                wordCounts.print();

                return jssc;
            }
        });

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
