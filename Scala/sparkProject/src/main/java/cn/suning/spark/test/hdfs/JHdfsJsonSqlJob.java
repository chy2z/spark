package cn.suning.spark.test.hdfs;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import java.util.*;

/**
 * @program: scalaspark
 * @description: 读取hdfs文件json数据
 * @author: 18093941
 * @create: 2019-03-07 16:36
 **/
public class JHdfsJsonSqlJob {

    private static Logger logger = Logger.getLogger(JHdfsJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;

    public static void main(String[] args) throws Exception {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JHdfsJsonSqlJob.class.getName(), args, 4);
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        //hdfs输入路径
        String inPath = "/people/people.json";
        String outPath = "/logs/result_log";
        String outSqlPath = "/logs/sql_log";

        HadoopHdfsUtil.deleteHdfsDir(fileSystem,hdfsBaseUrl+outPath);
        HadoopHdfsUtil.deleteHdfsDir(fileSystem,hdfsBaseUrl+outSqlPath);

        boolean flag= HadoopHdfsUtil.checkFileExists(fileSystem,hdfsBaseUrl+inPath);
        if(flag) {
            sqlContext.read().json(hdfsBaseUrl+inPath).registerTempTable("people");
            Dataset<Row> dataset=  sqlContext.sql("select * from people");
            dataset.printSchema();
            dataset.show(10);

            JavaRDD<Row> javaRDD= dataset.toJavaRDD();
            JavaRDD<String> result=javaRDD.mapPartitions(new FlatMapFunction<Iterator<Row>, String>() {
                @Override
                public Iterator<String> call(Iterator<Row> t) throws Exception {
                    List list = new LinkedList();
                    while (t.hasNext()) {
                        Row row = (Row)t.next();
                        String[] fields = row.schema().fieldNames();
                        Map record = new LinkedHashMap<String,Object>();
                        for (String field : fields) {
                            record.put(field,getValStrFilterNull(row,field));
                        }
                        list.add(JSONObject.toJSONString(record));
                    }
                    return list.iterator();
                }
            });
            /**
             *  写入文件
             * {"age":21,"name":"Michael"}
             */
            result.repartition(1).saveAsTextFile(hdfsBaseUrl+outPath);
            /**
             * 写入文件
             * {"age":21,"name":"Michael"}
             */
            dataset.write().mode(SaveMode.Overwrite).json(hdfsBaseUrl+outSqlPath);
        }
        else{
            logger.info("文件路径不正确");
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
        sparkConf.set("spark.default.parallelism", "200");
        sparkConf.set("spark.sql.shuffle.partitions", "200");
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

    public static String getValStrFilterNull(Row row, String name)
    {
        String val = "";
        try {
            val = row.getAs(name) + "";
            if ("null".equalsIgnoreCase(val))
                return "";
        }
        catch (Exception e) {
        }
        return val.trim();
    }

}
