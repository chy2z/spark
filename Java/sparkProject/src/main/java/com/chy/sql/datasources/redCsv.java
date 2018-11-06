package com.chy.sql.datasources;

import com.chy.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
* @Title: redCsv
* @Description: 读取csv文件
* @author chy
* @date 2018/5/22 8:53
*/
public class redCsv {

    public static void main(String[] args) {
        test2 ();
    }

    /**
     * 格式1：数据放在同1列
     */
    public static void test1 () {
        SparkSession spark = SparkUtil.getSparkSession();
        Dataset<Row> df = spark.read()
                .format("csv")
                //拆分符合
                .option("sep", ";")
                .option("inferSchema", "true")
                //是否有列头
                .option("header", "true")
                .load("src/main/resources/people.csv");
        df.show();

        Dataset<Row> result = df.select("name", "age", "job");

        result.show();
    }

    /**
     * 格式2:数据放在分布在不同列
     */
    public static void test2 () {
        SparkSession spark = SparkUtil.getSparkSession();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("src/main/resources/people2.csv");

        df.show();

        Dataset<Row> result = df.select("name", "age", "job");

        result.show();
    }

}
