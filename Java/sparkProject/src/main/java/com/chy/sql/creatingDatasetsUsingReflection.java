package com.chy.sql;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
* @Title: creatingDatasetsUsingReflection
* @Description: 使用反射将RDD转换成DataSet
* @author chy
* @date 2018/5/19 9:38
*/
public class creatingDatasetsUsingReflection {

    public static void main(String[] args) {
        SparkSession spark = SparkUtil.getSparkSession();

        /**
         * 创建Rdd
         */
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("src/main/resources/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person p = new Person();
                    p.setName(parts[0]);
                    p.setAge(Integer.parseInt(parts[1]));
                    return p;
                });

        /**
         * Rdd 转换 DataSet
         */
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);

        /**
         * 创建临时表
         */
        peopleDF.createOrReplaceTempView("people");


        /**
         * 查询
         */
        Dataset<Row> teenagersDF = spark.sql("SELECT name,age FROM people WHERE age BETWEEN 13 AND 19");

        /**
         * 展示数据
         */
        teenagersDF.show();

        /**
         * 通过字段索引访问
         */
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();

        /**
         * 通过字段名称访问
         */
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
    }

}
