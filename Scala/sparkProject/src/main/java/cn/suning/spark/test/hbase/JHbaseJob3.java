package cn.suning.spark.test.hbase;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.HbaseUtil;
import cn.suning.hadoop.PropertyUtil;
import cn.suning.spark.test.hdfs.JBroadcastJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;

    /**
     * @program: scalaspark
     * @description: 读取方式3
     * @author: 18093941
     * @create: 2019-03-11 15:48
     **/
    public class JHbaseJob3 {

        private static Logger logger = Logger.getLogger(JBroadcastJob.class);
        public static SparkSession sparkSession;
        public static JavaSparkContext javaSparkContext;
        public static FileSystem fileSystem;
        public static String hdfsBaseUrl;


        public static void main(String[] args) {
            PropertyUtil.setCommFlags("dev,");
            hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
            fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
            SparkConf sparkConf = initSparkConf(JHbaseJob3.class.getName(), args, 4);
            sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
            try {
                HbaseUtil hbaseCllent = HbaseUtil.getClient(HbaseUtil.DEFAULT_HBASE);
                hbaseCllent.getResultScann("person");
            } catch (IOException e) {
               logger.error(e);
            }
            HadoopHdfsUtil.closeFileSystem(fileSystem);
            closeJavaSparkContext(javaSparkContext);
            closeSparkSession(sparkSession);
        }

        public static SparkConf initSparkConf(String className, String[] args, int length) {
            SparkConf sparkConf = new SparkConf().setAppName(className);
            if (PropertyUtil.RunEnvFlagEnum.DEV == PropertyUtil.getRunEnvFlag())
                sparkConf.setMaster("local[1]");
            else {
                sparkConf.setMaster("yarn");
            }
            sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            sparkConf.set("spark.kryo.registrator", "cn.suning.spark.test.hdfs.MyRegistrator");
            sparkConf.set("spark.shuffle.consolidateFiles", "true");
            sparkConf.set("spark.default.parallelism", "200");
            sparkConf.set("spark.sql.shuffle.partitions", "200");
            return sparkConf;
        }

        public static void closeJavaSparkContext(JavaSparkContext javaSparkContext) {
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
