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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import java.util.*;

public class JBroadcastJob {
    private static Logger logger = Logger.getLogger(JBroadcastJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;

    public static void main(String[] args) throws Exception {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JBroadcastJob.class.getName(), args, 4);
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        //hdfs输入路径
        String inPath = "/people/people.json";
        boolean flag= HadoopHdfsUtil.checkFileExists(fileSystem,hdfsBaseUrl+inPath);
        if(flag) {
            sqlContext.read().json(hdfsBaseUrl+inPath).registerTempTable("people");
            Dataset<Row> dataset=  sqlContext.sql("select * from people");
            dataset.printSchema();
            dataset.show(10);

            Map<String,String> map=new HashMap<>();
            map.put("Michael","1");
            map.put("Justin","1");
            final Broadcast filterMapBroadcast = javaSparkContext.broadcast(map);

            JavaRDD<Row> javaRDD= dataset.toJavaRDD();

            JavaRDD<String> result=javaRDD.filter(new Function<Row, Boolean>() {
                final Map<String, String> brandMap = (Map)filterMapBroadcast.getValue();
                @Override
                public Boolean call(Row row) throws Exception {
                    String name = getValStrFilterNull(row, "name");
                    return brandMap.containsKey(name);
                }
            }).mapPartitions(new FlatMapFunction<Iterator<Row>, String>() {
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

            result.foreach(s->{
                logger.info("结果----->"+s);
            });
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
        //设置序列化使用的库
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //在该库中注册用户定义的类型
        sparkConf.set("spark.kryo.registrator", "cn.suning.spark.test.hdfs.MyRegistrator");
        //rdd默认不压缩，设置可以开启压缩功能,使用压缩机制，也会增加额外的开销，也会影响到性能
        //sparkConf.set("spark.rdd.compress", "true");
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
