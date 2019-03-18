package cn.suning.spark.demo1.util;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;


public class SelectHdfsRepeatCountUtil {
    private static Logger logger = Logger.getLogger(SelectHdfsRepeatCountUtil.class);
    private static String hdfsBaseUrl;
    private static final String CLASS_NAME = new Object() {
        public String getClassName() {
            String clazzName = getClass().getName();
            return clazzName.substring(clazzName.lastIndexOf('.') + 1, clazzName.lastIndexOf('$'));
        }
    }.getClassName();

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(new Option("e", "env", true, "环境(dev pre prd)") { });
        options.addOption(new Option("i", "inpath", true, "数据路径") { });
        options.addOption(new Option("k", "keyindex", true, "组合主键位置") { });
        options.addOption("s", "separator", true, "分隔符,默认是  制表符");

        CommandLineParser parser = new BasicParser();
        CommandLine commandLine = parser.parse(options, args);
        String env = commandLine.getOptionValue('e');
        String inpath = commandLine.getOptionValue('i');
        String keyindex = commandLine.getOptionValue('k');
        String separator = commandLine.getOptionValue('s', "\t");

        PropertyUtil.setCommFlags(env);

        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");

        SparkConf sparkConf = new SparkConf().setAppName("DPS_" + CLASS_NAME + System.currentTimeMillis());
        if (PropertyUtil.RunEnvFlagEnum.DEV == PropertyUtil.getRunEnvFlag()) {
            sparkConf.setMaster("local[1]");
        }

        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "com.suning.search.dps.util.RryoRegistrator");

        sparkConf.set("spark.shuffle.consolidateFiles", "true");

        sparkConf.set("spark.shuffle.memoryFraction", "0.7");
        sparkConf.set("spark.storage.memoryFractionction", "0.2");
        sparkConf.set("spark.default.parallelism", "200");
        sparkConf.set("spark.sql.shuffle.partitions", "200");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(javaSparkContext);

        SparkUtil.registerMyFuns(sqlContext);

        FileSystem fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        try {
            String[] keyIndexStrs = keyindex.split(",", -1);
            int keyIndexLen = keyIndexStrs.length;
            int[] keyIntIndex = new int[keyIndexLen];
            for (int i = 0; i < keyIndexLen; i++) {
                keyIntIndex[i] = Integer.valueOf(keyIndexStrs[i]).intValue();
            }

            JavaRDD javaRDD = javaSparkContext.textFile(hdfsBaseUrl + inpath).mapPartitionsToPair(new PairFlatMapFunctionTmp(separator, keyIntIndex, keyIntIndex.length)).reduceByKey(new Function2<Integer,Integer,Integer>() {
                private static final long serialVersionUID = 1L;

                public Integer call(Integer v1, Integer v2) throws Exception {
                    return Integer.valueOf(v1.intValue() + v2.intValue());
                }
            }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
                private static final long serialVersionUID = 1L;

                public Boolean call(Tuple2<String, Integer> v1)
                        throws Exception {
                    return Boolean.valueOf(((Integer) v1._2).intValue() > 1);
                }
            }).map(new Function<Tuple2<String, Integer>, String>() {
                private static final long serialVersionUID = 1L;

                public String call(Tuple2<String, Integer> v1)
                        throws Exception {
                    return v1._2 + "\t" + (String) v1._1;
                }
            });
            long len = javaRDD.count();

            List logList = new LinkedList();

            if (len >= 500L) {
                logList.add("查找重复  路径为：" + hdfsBaseUrl + inpath);
                List list2 = javaRDD.take(500);
                for (int i = 0; i < 500; i++)
                    logList.add(list2.get(i));
            } else if (len == 0L) {
                logList.add("查找重复 路径为：" + hdfsBaseUrl + inpath + "-->" + (String) javaRDD.take(1).get(0));
            } else {
                logList.add("查找重复 路径为：" + hdfsBaseUrl + inpath);
                List list2 = javaRDD.collect();
                for (int i = 0; i < len; i++) {
                    logList.add(list2.get(i));
                }
            }
            logList.add("查找重复--查找到总条数:" + len);

            SparkUtil.log(hdfsBaseUrl, javaSparkContext, logList);
        } catch (Exception e) {
            logger.error("计算出错", e);
            throw e;
        } finally {
            HadoopHdfsUtil.closeFileSystem(fileSystem);
            SparkUtil.closeJavaSparkContext(javaSparkContext);
        }
    }
}
