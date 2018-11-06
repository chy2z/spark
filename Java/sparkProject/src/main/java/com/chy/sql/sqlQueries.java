package com.chy.sql;

import com.chy.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
* @Title: sqlQueries
* @Description: sql 查询
* @author chy
* @date 2018/5/18 15:05
*/
public class sqlQueries {

    public static void main(String[] args){

        SparkSession spark= SparkUtil.getSparkSession();

        Dataset<Row> df = spark.read().json("src/main/resources/people.json");

        //显示所有数据
        df.show();

        //注册临时视图
        df.createOrReplaceTempView("people");

        //查询数据
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");

        //数据显示
        sqlDF.show();
    }
}
