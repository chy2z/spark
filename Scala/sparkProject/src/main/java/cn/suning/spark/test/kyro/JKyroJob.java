package cn.suning.spark.test.kyro;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import cn.suning.spark.test.hdfs.JBroadcastJob;
import cn.suning.spark.test.hdfs.PersonPojo;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

/**
 * @program: scalaspark
 * @description:
 * @author: 18093941
 * @create: 2019-03-11 20:05
 **/
public class JKyroJob {
    private static Logger logger = Logger.getLogger(JBroadcastJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;

    public static void main(String[] args) throws Exception {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JKyroJob.class.getName(), args, 4);
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        final Broadcast<PersonPojo> personPojoBroadcast = javaSparkContext.broadcast(new PersonPojo("苏宁",21));
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("手机","电脑","洗衣机"));

        JavaPairRDD rdd2 = rdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                Tuple2<String,String> tuple2=new Tuple2(s,personPojoBroadcast.getValue().getName());
                return tuple2;
            }
        });

        JavaRDD rdd3 = rdd.map(new Function<String,PersonPojo>() {
            @Override
            public PersonPojo call(String v) throws Exception {
                return new PersonPojo(v,personPojoBroadcast.getValue().getAge());
            }
        });

        rdd2.foreach(s->{
            Tuple2<String,String> tuple=(Tuple2<String,String>)s;
            logger.info("Tuple2--------->"+tuple._1+":"+tuple._2);
        });

        rdd3.foreach(s->{
            PersonPojo personPojo=(PersonPojo)s;
            logger.info("PersonPojo--------->"+personPojo.getName()+":"+personPojo.getAge());
        });

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
        //设置序列化使用的库
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //在该库中注册用户定义的类型
        sparkConf.set("spark.kryo.registrator", "cn.suning.spark.test.hdfs.MyRegistrator");
        //rdd默认不压缩，设置可以开启压缩功能,使用压缩机制，也会增加额外的开销，也会影响到性能
        //sparkConf.set("spark.rdd.compress", "true");
        sparkConf.set("spark.shuffle.consolidateFiles", "true");
        //该参数用于设置每个stage的默认task数量(只有在处理RDD时才会起作用，对Spark SQL的无效)
        sparkConf.set("spark.default.parallelism", "100");
        //该参数代表了shuffle read task的并行度(则是对sparks SQL专用的设置)
        sparkConf.set("spark.sql.shuffle.partitions", "100");
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

}
