package cn.suning.spark.test.hbase;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import cn.suning.spark.test.hdfs.JBroadcastJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import java.util.*;

/**
 * @program: scalaspark
 * @description: 批量插入
 * @author: 18093941
 * @create: 2019-03-11 16:50
 **/
public class JHbaseBatchPutJob {

    private static Logger logger = Logger.getLogger(JBroadcastJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;

    public static void main(String[] args)  throws Exception {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JHbaseBatchPutJob.class.getName(), args, 4);
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        String tableName = "person";
        Map<String, String> hbaseConf = new HashMap();
        hbaseConf.put("hbase.client.operation.timeout","3000");
        hbaseConf.put("hbase.client.scanner.timeout.period","600000");
        hbaseConf.put("hbase.rootdir", PropertyUtil.getProperty("hbase_rootdir", ""));
        hbaseConf.put("hbase.zookeeper.quorum", PropertyUtil.getProperty("hbase_zookeeper_quorum", ""));
        hbaseConf.put("hbase.zookeeper.property.clientPort", PropertyUtil.getProperty("hbase_zookeeper_property_clientPort", ""));

        //map,list
        final Broadcast cfgBroadcast = javaSparkContext.broadcast(hbaseConf);

        //hdfs输入路径
        String inPath = "/people/people.json";
        boolean flag= HadoopHdfsUtil.checkFileExists(fileSystem,hdfsBaseUrl+inPath);
        if(flag) {
            sqlContext.read().json(hdfsBaseUrl + inPath).registerTempTable("people");
            Dataset<Row> dataset = sqlContext.sql("select * from people");
            dataset.show(10);
            JavaRDD<Row> javaRDD = dataset.toJavaRDD();
            /* lambda-表达式写法
            javaRDD.mapPartitions(rowIterator->{
                while (rowIterator.hasNext()){

                }
                return new ArrayList().iterator();
            });
            */
            JavaRDD result = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Row>, Object>() {
                @Override
                public Iterator<Object> call(Iterator<Row> rowIterator) throws Exception {
                    Configuration cfg = new Configuration();
                    Map<String, String> hbaseConf = (Map<String, String>) cfgBroadcast.getValue();
                    for (Map.Entry<String, String> ent : hbaseConf.entrySet()) {
                        cfg.set((String) ent.getKey(), (String) ent.getValue());
                    }
                    Connection conn = null;
                    try {
                        conn = ConnectionFactory.createConnection(cfg);
                        Table table = conn.getTable(TableName.valueOf(tableName));
                        List mutations = new ArrayList();
                        while (rowIterator.hasNext()) {
                            Row row = rowIterator.next();
                            String name = getValStrFilterNull(row, "name");
                            String age = getValStrFilterNull(row, "age");
                            Put put = new Put(Bytes.toBytes(name));
                            put.setDurability(Durability.SYNC_WAL);
                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age));
                            mutations.add(put);
                            if (mutations.size() >= 20) {
                                table.batch(mutations);
                                mutations.clear();
                            }
                        }
                        if (mutations.size() >= 0) {
                            table.batch(mutations);
                            mutations.clear();
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                    finally {
                        org.apache.commons.io.IOUtils.closeQuietly(conn);
                    }
                    return new ArrayList().iterator();
                }
            });
            result.count();
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
