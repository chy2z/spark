package com.chy.streaming;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @program: snDemo
 * @description:
 * @author: 18093941
 * @create: 2018-10-31 17:10
 **/
public class ProducterSample implements Runnable {

    private boolean isAsync=true;

    private Producer<String, String> producer;

    private String brokers;

    private String topic;

    public static final String ZOOKEEPER_HOSTS = "localhost:2181";

    public static final String BROKER_HOSTS = "localhost:9092";

    public static final String TOPIC = "spark_topic";

    public static void main(String[] args) {

        Thread th = new Thread(new ProducterSample(BROKER_HOSTS,TOPIC));

        th.start();

        try {
            th.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 生产者生产数据
     * 发送消息是异步进行，一旦消息被保存到分区缓存中，send方法就返回
     * 一旦消息被接收 就会调用callBack
     * @param topic
     */
    public ProducterSample(String brokers,String topic) {
        this.brokers = brokers;
        this.topic = topic;
        Properties props = new Properties();
        //kafka集群机器
        props.put("bootstrap.servers", brokers);
        //生产者发送的数据需要等待主分片和其副本都保存才发回确认消息
        props.put("acks", "all");
        //生产者发送失败后的确认消息
        props.put("retries", 0);
        //生产者 每个分区缓存大小 16K
        props.put("batch.size", 16384);
        //生产者发送分区缓存中数据前停留时间
        props.put("linger.ms", 1);
        //生产者可用缓存总量大小 32M
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<String, String>(props);
    }

    /**
     *消息被保存之后的回调方法
     */
    class Call implements Callback {

        private final String key;

        private final String value;

        public Call(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void onCompletion(RecordMetadata recordmetadata, Exception exception) {
            System.out.println("callBack: partition "+recordmetadata.partition()+" recordmetadata offset " + recordmetadata.offset() + " recordmetadata content : " + value);
        }

    }

    @Override
    public void run() {
        while (true) {
            try {
                for (int j = 1; j <= 100; j++) {
                    System.out.println("第"+j+"次");
                    for (int i = 0; i < 10; i++) {
                        String key = null;
                        String value = Integer.toString(i+10);
                        //发送消息是异步进行，一旦消息被保存到分区缓存中，send方法就返回
                        if (isAsync) {
                            producer.send(new ProducerRecord<String, String>(topic, key, value), new Call(key, value));
                        } else {
                            producer.send(new ProducerRecord<String, String>(topic, key, value));
                        }
                    }
                    Thread.sleep(10000);
                }
            }
            catch (Exception ex){
                System.out.println(ex.getMessage());
            }
            finally {
                producer.close();
            }
        }
    }
}
