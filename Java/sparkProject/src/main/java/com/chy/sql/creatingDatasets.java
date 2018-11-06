package com.chy.sql;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
* @Title: creatingDatasets
* @Description: 创建Dataset
* @author chy
* @date 2018/5/18 15:20
*/
public class creatingDatasets {

    public static void main(String[] args){

        SparkSession spark= SparkUtil.getSparkSession();

        Person person1 = new Person();
        person1.setName("Andy");
        person1.setAge(32);

        Person person2 = new Person();
        person2.setName("Cndy");
        person2.setAge(25);

        List<Person> listPerson=new ArrayList<>();
        listPerson.add(person1);
        listPerson.add(person2);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                //单个
                //Collections.singletonList(person1),
                //集合
                listPerson,
                personEncoder
        );
        javaBeanDS.show();


        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect();
        // Returns [2, 3, 4]

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        String path = "src/main/resources/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
    }

}
