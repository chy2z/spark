package cn.suning.spark.test.hbase;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import cn.suning.spark.test.hdfs.JBroadcastJob;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: scalaspark
 * @description: 读取方式1
 * @author: 18093941
 * @create: 2019-03-11 15:26
 **/
public class JHbaseJob1 {

    private static Logger logger = Logger.getLogger(JBroadcastJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;


    public static void main(String[] args) {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JHbaseJob1.class.getName(), args, 4);
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        Configuration cfg = new Configuration();
        cfg.set("hbase.client.operation.timeout","3000");
        cfg.set("hbase.client.scanner.timeout.period","600000");
        cfg.set("hbase.rootdir", PropertyUtil.getProperty("hbase_rootdir", ""));
        cfg.set("hbase.zookeeper.quorum", PropertyUtil.getProperty("hbase_zookeeper_quorum", ""));
        cfg.set("hbase.zookeeper.property.clientPort", PropertyUtil.getProperty("hbase_zookeeper_property_clientPort", ""));

        Configuration config = HBaseConfiguration.create(cfg);
        Connection conn = null;
        String tableName="person";
        try {
            conn = ConnectionFactory.createConnection(config);
            getResultScann(conn, tableName);
        } catch (IOException e) {
            logger.error(e);
        }
        finally {
            org.apache.commons.io.IOUtils.closeQuietly(conn);
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


    public static void getResultScann(Connection conn,String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.rawCells()) {
                    Map<String,String> map=new HashMap<>();
                    map.put("Rowkey", Bytes.toString(r.getRow()));
                    map.put("RowName",new String(CellUtil.cloneRow(cell)));
                    map.put("Timetamp",cell.getTimestamp()+"");
                    map.put("ColumnFamily",new String(CellUtil.cloneFamily(cell)));
                    map.put("ColName",new String(CellUtil.cloneQualifier(cell)));
                    map.put("Value",new String(CellUtil.cloneValue(cell)));
                    logger.info(JSONObject.toJSONString(map));
                }
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

}
