package cn.suning.spark.test.hdfs;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @program: scalaspark
 * @description: 拷贝读取hdfs文件内容
 * @author: 18093941
 * @create: 2019-03-07 10:08
 **/
public class JHdfsJob {
    private static Logger logger = Logger.getLogger(JHdfsJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;

    public static void main(String[] args) throws Exception {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JHdfsJob.class.getName(), args, 4);
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        int numPartitions = 2;
        //String inPath = args[2];
        //String outPath = args[3];
        //hdfs输入路径
        String inPath = "/sql/sql.txt";
        //hdfs写入路径
        String outPath = "/copy/JHdfsJob";
        boolean isZip = false;
        HadoopHdfsUtil.deleteHdfsDir(fileSystem, hdfsBaseUrl + outPath);
        if (isZip)
            javaSparkContext.textFile(hdfsBaseUrl + inPath).repartition(numPartitions).saveAsTextFile(hdfsBaseUrl + outPath, GzipCodec.class);
        else {
            javaSparkContext.textFile(hdfsBaseUrl + inPath).repartition(numPartitions).saveAsTextFile(hdfsBaseUrl + outPath);
        }

        //判断文件是否写入成功
        boolean flag= HadoopHdfsUtil.checkHdfsSuccess(fileSystem,hdfsBaseUrl+outPath);

        if(flag) {
            logger.info("文件保存成功");

            JavaRDD<String> textFileRdd = javaSparkContext.textFile(hdfsBaseUrl+outPath);

            //过滤
            textFileRdd= textFileRdd.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    String[] list=s.split("\\t");
                    return list.length==4;
                }
            });

            //打印每行记录
            textFileRdd.foreach(new VoidFunction<String>() {
                @Override
                public void call(String s) throws Exception {
                    String[] list=s.split("\\t");
                    Arrays.stream(list).forEach((str)->{
                        logger.info("内容分词---->"+str);
                    });
                }
            });

            //flatMap 将行数据切分为单词
            JavaRDD<String> words=textFileRdd.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterator<String> call(String s) throws Exception {
                    return Arrays.asList(s.split("\\t")).iterator();
                }
            });

            //合并
            JavaPairRDD<String,Integer> result=words.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2(s,1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                //合并具有相同键的值
                @Override
                public Integer call(Integer a, Integer b) throws Exception {
                    //键相同,则对应的值相加
                    return a+b;
                }
            });

            JavaRDD<String> javaRDD= result.map(new Function<Tuple2<String,Integer>, String>() {
                @Override
                public String call(Tuple2<String, Integer> t) throws Exception {
                    logger.info("统计结果:"+t._1+"----->"+t._2);
                    return t._1;
                }
            });

            result.collect().stream().forEach(t->{
                 logger.info("统计结果:"+t._1+"----->"+t._2);
            });
        }
        else{
            logger.info("文件保存失败");
        }

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
        if (PropertyUtil.RunEnvFlagEnum.PRD == PropertyUtil.getRunEnvFlag()) {
            sparkConf.set("spark.default.parallelism", "900");
            sparkConf.set("spark.sql.shuffle.partitions", "900");
        }
        else{
            sparkConf.set("spark.default.parallelism", "200");
            sparkConf.set("spark.sql.shuffle.partitions", "200");
        }
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
