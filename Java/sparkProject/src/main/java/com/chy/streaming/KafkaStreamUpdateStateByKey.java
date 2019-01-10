package com.chy.streaming;

import com.chy.util.SparkUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 以DStream中的数据进行按key做reduce操作，然后对各个批次的数据进行累加
 在有新的数据信息进入或更新时。能够让用户保持想要的不论什么状。使用这个功能须要完毕两步：
 1) 定义状态：能够是随意数据类型
 2) 定义状态更新函数：用一个函数指定怎样使用先前的状态。从输入流中的新值更新状态。
 对于有状态操作，要不断的把当前和历史的时间切片的RDD累加计算，随着时间的流失，计算的数据规模会变得越来越大。
 */
public class KafkaStreamUpdateStateByKey {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        String brokers="localhost:9092";
        String topics = "spark_topic";
        String groupid = "spark_streaming_group";

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id", groupid);
        //程序重新启动后从最老的加载，数据重复
        //kafkaParams.put("auto.offset.reset", "smallest");
        //程序重新启动后从最新的加载，数据丢失
        kafkaParams.put("auto.offset.reset", "largest");

        String directory="src/main/resources/checkpoint/KafkaStreamUpdateStateByKey";

        /**
         * checkpoint用法：
         * 1： 不存在checkpoint目录时，创建新的JavaStreamingContext，同时编写 dstream 业务代码
         * 2： 当程序终止在次运行程序时，发现checkpoint目录存在，通过checkpoint恢复程序运行，记住不需要再次执行 dstream 业务代码，否则会报
         *     org.apache.spark.SparkException: org.apache.spark.streaming.dstream.FlatMapp@5a69b104 has not been initialized，
         *     所以 dstream 业务代码 只需要在创建新的JavaStreamingContext时执行一次就够了！！！！切记！！！
         */
        JavaStreamingContext jssc=JavaStreamingContext.getOrCreate(directory, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                JavaStreamingContext jssc = SparkUtil.getJavaStreamingContext(10000);
                //设置检查点保存路径
                jssc.checkpoint(directory);

                JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicsSet
                );

                //设置检查点保存时间
                messages.checkpoint(new Duration(10000));

                JavaDStream<String> lines = messages.map(Tuple2::_2);

                JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

                words.print();

                JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                        .updateStateByKey(new Function2<List<Integer>, org.apache.spark.api.java.Optional<Integer>, org.apache.spark.api.java.Optional<Integer>>() {
                            @Override
                            public org.apache.spark.api.java.Optional<Integer> call(List<Integer> values, org.apache.spark.api.java.Optional<Integer> state) throws Exception {
                                //第一个参数就是key传进来的数据，第二个参数是曾经已有的数据
                                //如果第一次，state没有，updatedValue为0，如果有，就获取
                                Integer updatedValue = 0 ;
                                if(state.isPresent()){
                                    updatedValue = state.get();
                                }

                                //遍历batch传进来的数据可以一直加，随着时间的流式会不断去累加相同key的value的结果。
                                for(Integer value: values){
                                    updatedValue += value;
                                }

                                //返回更新的值
                                return Optional.of(updatedValue);
                            }
                        });

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
