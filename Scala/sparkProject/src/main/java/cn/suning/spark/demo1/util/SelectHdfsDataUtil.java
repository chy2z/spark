package cn.suning.spark.demo1.util;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class SelectHdfsDataUtil extends CommonSparkTemplate
{
    private static final long serialVersionUID = 6346339235787613037L;
    private static Logger logger = Logger.getLogger(SelectHdfsDataUtil.class);
    private static String pathStr;
    private static String findStr;

    public static void main(String[] args)
            throws Exception
    {
        SparkConf sparkConf = initSparkConf(getClassName(SelectHdfsDataUtil.class), args, 3);
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        sparkConf.set("spark.shuffle.memoryFraction", "0.7");
        sparkConf.set("spark.storage.memoryFractionction", "0.2");
        sparkConf.set("spark.default.parallelism", "200");
        sparkConf.set("spark.sql.shuffle.partitions", "200");
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        pathStr = args[1];
        findStr = args[2];
        logger.info("runEnvFlag:" + PropertyUtil.getRunEnvFlag());
        logger.info("hdfsBaseUrl:" + PropertyUtil.getProperty("hdfsBaseUrl"));
        logger.info("zkIps:" + PropertyUtil.getProperty("zkIps"));
        try {
            List<String> findList = new ArrayList();
            findList.add(findStr);
            Broadcast broadcast = javaSparkContext.broadcast(findList);
            logger.info("查找:" + findStr + " 路径为：" + hdfsBaseUrl + pathStr);
            JavaRDD allDataRdd = javaSparkContext.textFile(hdfsBaseUrl + pathStr);

            JavaRDD javaRDD = allDataRdd.mapPartitions(new FlatMapFunction<Iterator<String>,String>() {
                private static final long serialVersionUID = -4205596865575040853L;

                public Iterator<String> call(Iterator<String> t) throws Exception {
                    List list = new ArrayList();
                    String line;
                    while (t.hasNext()) {
                        line = (String)t.next() + "";
                        List<String> findStrList = (List)broadcast.getValue();
                        for (String findStr : findStrList) {
                            if (line.contains(findStr)) {
                                list.add(line);
                            }
                        }
                    }
                    return list.iterator();
                }
            });
            long len = javaRDD.count();

            List logList = new LinkedList();

            if (len >= 50000L) {
                logList.add("--开始查找: " + findStr + " 查找路径为: " + hdfsBaseUrl + pathStr);
                List list2 = javaRDD.take(10000);
                for (int i = 0; i < 10000; i++)
                    logList.add(list2.get(i));
            }
            else if (len == 0L) {
                logList.add("--开始查找: " + findStr + " 查找路径为: " + hdfsBaseUrl + pathStr + " -->" + (String)allDataRdd.take(1).get(0));
            } else {
                logList.add("--开始查找: " + findStr + " 查找路径为: " + hdfsBaseUrl + pathStr);
                List list2 = javaRDD.collect();
                for (int i = 0; i < len; i++) {
                    logList.add(list2.get(i));
                }
            }
            logList.add("--查找到总条数: " + len + " 原数据总条数: " + allDataRdd.count());

            JavaRDD javaRDD2 = javaSparkContext.parallelize(logList);
            saveAsTextFileCoalesce(javaRDD2, 1, 20, "/user/sousuo/data/dps/find/", 2);
            HadoopHdfsUtil.closeFileSystem(fileSystem);
            SparkUtil.closeJavaSparkContext(javaSparkContext);
            SparkUtil.closeSparkSession(sparkSession);
        } catch (Exception e) {
            logger.error("查找出错", e);
            throw e;
        }
    }
}