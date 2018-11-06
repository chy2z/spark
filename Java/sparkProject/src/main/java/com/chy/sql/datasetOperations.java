package com.chy.sql;

import com.chy.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
* @Title: datasetOperations
* @Description: Dataset  操作
* @author chy
* @date 2018/5/18 15:04
*/
public class datasetOperations {

    public static void main(String[] args){

        SparkSession spark= SparkUtil.getSparkSession();

        Dataset<Row> df = spark.read().json("src/main/resources/people.json");

        //显示所有数据
        df.show();

        //显示字段信息
        df.printSchema();

        //选择name属性列
        df.select("name").show();

        //选择name属性列和age属性列，并且age加1
        df.select(col("name"), col("age").plus(1)).show();

        //选择年龄大于21岁
        df.filter(col("age").gt(21)).show();

        //age分组求和
        df.groupBy("age").count().show();
    }
}
