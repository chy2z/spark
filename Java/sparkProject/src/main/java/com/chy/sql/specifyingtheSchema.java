package com.chy.sql;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chy on 2018/5/19.
 */
public class specifyingtheSchema {
    public static void main(String[] args) {
        SparkSession spark = SparkUtil.getSparkSession();

        /**
         * 创建Rdd
         */
        JavaRDD<String> peopleRDD = spark.read()
                .textFile("src/main/resources/people.txt")
                .toJavaRDD();

        /**
         * string结构模式
         */
        String schemaString = "name age";


        /**
         * 依据string结构模式创建结构模式
         */
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        /**
         * 将 Rdd 转换成 Row
         */
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        /**
         * 应用模式到RDD 转成 DataSet
         */
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        /**
         * 创建临时表
         */
        peopleDataFrame.createOrReplaceTempView("people");

        /**
         * 查询记录
         */
        Dataset<Row> results = spark.sql("SELECT name,age FROM people");

        results.show();

        /**
         * 获取名字
         */
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());

        namesDS.show();
    }
}