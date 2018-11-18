package com.chy.sql.datasources;

import com.chy.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
* @Title: redHdfs
* @Description: 读取hdfs
* @author chy
* @date 2018/11/19 0:42
*/
public class redHdfs {

    public static void main(String[] args) {
        test1();
    }


    public static void test1 () {
        SparkSession spark = SparkUtil.getSparkSession();
        Dataset<Row> df = spark.read()
                .format("csv")
                //拆分符合
                .option("sep", ";")
                .option("inferSchema", "true")
                //是否有列头
                .option("header", "true")
                .load("hdfs://localhost:9000/chy-data/user.csv");

        df.show();
    }
}
