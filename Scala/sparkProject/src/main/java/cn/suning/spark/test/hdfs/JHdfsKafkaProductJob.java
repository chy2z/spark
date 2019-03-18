package cn.suning.spark.test.hdfs;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @program: scalaspark
 * @description:
 * @author: 18093941
 * @create: 2019-03-07 15:56
 **/
public class JHdfsKafkaProductJob {
    private static Logger logger = Logger.getLogger(JHdfsJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;
    public static Producer<String, String> producer;
    public static String brokers;
    public static String topic;

    public static void main(String[] args) throws Exception {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JHdfsKafkaProductJob.class.getName(), args, 4);
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        //hdfs路径
        String inPath = "/sql/sql.txt";
        //判断文件是否存在
        boolean flag= HadoopHdfsUtil.checkFileExists(fileSystem,hdfsBaseUrl+inPath);

        if(flag) {
            JavaRDD<String> textFileRdd = javaSparkContext.textFile(hdfsBaseUrl+inPath);

            brokers="localhost:9092";
            topic="hdfs_sql";
            producer=initKafkaProducer(brokers);

            textFileRdd.foreach(new VoidFunction<String>() {
                @Override
                public void call(String s) throws Exception {
                    producer.send(new ProducerRecord<String, String>(topic, null, s));
                    logger.info("发送消息---->"+s);
                }
            });
        }
        else{
            logger.info("文件不存在");
        }

        producer.close();
        HadoopHdfsUtil.closeFileSystem(fileSystem);
        closeJavaSparkContext(javaSparkContext);
        closeSparkSession(sparkSession);
    }

    public static SparkConf initSparkConf(String className, String[] args, int length)
    {
        SparkConf sparkConf = new SparkConf().setAppName(className);
        if (PropertyUtil.RunEnvFlagEnum.DEV == PropertyUtil.getRunEnvFlag())
            sparkConf.setMaster("local[1]");
        else {
            sparkConf.setMaster("yarn");
        }
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "cn.suning.spark.test.hdfs.MyRegistrator");
        sparkConf.set("spark.shuffle.consolidateFiles", "true");
        sparkConf.set("spark.default.parallelism", "600");
        sparkConf.set("spark.sql.shuffle.partitions", "600");
        return sparkConf;
    }

    public static void closeJavaSparkContext(JavaSparkContext javaSparkContext)
    {
        if (null != javaSparkContext) {
            javaSparkContext.stop();
            javaSparkContext.close();
        }
    }

    public static void closeSparkSession(SparkSession sparkSession) {
        if (null != sparkSession) {
            sparkSession.stop();
            sparkSession.close();
        }
    }

    public static KafkaProducer initKafkaProducer(String brokers) {
        Properties props = new Properties();
        //kafka集群机器
        props.put("bootstrap.servers", brokers);
        //生产者发送的数据需要等待主分片和其副本都保存才发回确认消息
        props.put("acks", "1");
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
        return new KafkaProducer<String, String>(props);
    }

}
