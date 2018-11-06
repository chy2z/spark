package com.chy.sql.datasources;

import com.chy.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
* @Title: redJson
* @Description:
* @author chy
* @date 2018/5/22 8:37
*/
public class redJson {

    public static void main(String[] arg){

        SparkSession spark= SparkUtil.getSparkSession();

        //Dataset<Row> df = spark.read().json("src/main/resources/people.json");

        Dataset<Row> df = spark.read().format("json").load("src/main/resources/people.json");

        df.show();

        df.printSchema();

        Dataset<Row> filter=df.filter("age > 15");

        filter.show();

        Dataset<Row> groupBy=df.groupBy("name").count();

        groupBy.show();


        Dataset<Row> result= df.select("name","age");

        result.show();

        result.write().format("parquet").save("src/main/resources/namesAndAges.parquet");
    }

}
