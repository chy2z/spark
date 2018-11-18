package com.chy.sql.datasources;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
* @Title: redHive
* @Description: 读取hive
* @author chy
* @date 2018/11/19 0:43
*/
public class redHive {

    public static void main(String[] args) {
        test2();
    }

    /**
     * 访问hive源
     */
    public static void test1(){
        SparkSession spark = SparkUtil.getSparkSessionForHive();
        spark.sql("show tables").show();
        spark.sql("select * from test1").show();
        spark.stop();
    }


    /**
     * 访问hive源
     * 创建表
     * 导入记录
     * 统计就记录
     */
    public static void test2(){
        SparkSession spark = SparkUtil.getSparkSessionForHive();
        spark.sql("CREATE TABLE IF NOT EXISTS kvsrc (key INT, value STRING) USING hive");
        spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE kvsrc");

        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM kvsrc").show();
        // +---+-------+
        // |key|  value|
        // +---+-------+
        // |238|val_238|
        // | 86| val_86|
        // |311|val_311|
        // ...

        // Aggregation queries are also supported.
        spark.sql("SELECT COUNT(*) FROM kvsrc").show();
        // +--------+
        // |count(1)|
        // +--------+
        // |    500 |
        // +--------+

        // The results of SQL queries are themselves DataFrames and support all normal functions.
        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM kvsrc WHERE key < 10 ORDER BY key");

        // The items in DataFrames are of type Row, which lets you to access each column by ordinal.
        Dataset<String> stringsDS = sqlDF.map(
                (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
                Encoders.STRING());
        stringsDS.show();
        // +--------------------+
        // |               value|
        // +--------------------+
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // ...

        // You can also use DataFrames to create temporary views within a SparkSession.
        List<Record> records = new ArrayList<>();
        for (int key = 1; key < 100; key++) {
            Record record = new Record();
            record.setKey(key);
            record.setValue("val_" + key);
            records.add(record);
        }
        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

        // Queries can then join DataFrames data with data stored in Hive.
        spark.sql("SELECT * FROM records r JOIN kvsrc s ON r.key = s.key").show();
        // +---+------+---+------+
        // |key| value|key| value|
        // +---+------+---+------+
        // |  2| val_2|  2| val_2|
        // |  2| val_2|  2| val_2|
        // |  4| val_4|  4| val_4|
        // ...
        // $example off:spark_hive$

        spark.stop();
    }


    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
