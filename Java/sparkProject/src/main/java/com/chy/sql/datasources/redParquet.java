package com.chy.sql.datasources;

import com.chy.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
* @Title: redParquet
* @Description: 读取和创建Parquet文件
* @author chy
* @date 2018/5/22 8:11
*/
public class redParquet {

    public static void main(String[] arg){
        SparkSession spark= SparkUtil.getSparkSession();

        Dataset<Row> df = spark.read().parquet("src/main/resources/users.parquet");

        //Dataset<Row> df = spark.read().load("src/main/resources/users.parquet");

        df.show();

        Dataset<Row> result= df.select("name","favorite_color");

        result.show();

        result.write().save("src/main/resources/namesAndFavColors.parquet");
    }

}
