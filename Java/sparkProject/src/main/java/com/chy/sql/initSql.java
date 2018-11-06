package com.chy.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
* @Title: initSql
* @Description: spark initSql
* @author chy
* @date 2018/5/18 10:07 
*/
public class initSql {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQL")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("src/main/resources/people.json");

        //显示所有数据
        df.show();
    }
}