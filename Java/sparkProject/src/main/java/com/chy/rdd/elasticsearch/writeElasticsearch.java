package com.chy.rdd.elasticsearch;

import com.chy.util.SparkUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.Serializable;
import java.util.Map;

public class writeElasticsearch {

    public static void main(String[] args) {
        test4();
    }

    public static void test1(){
        JavaSparkContext sc = SparkUtil.getJavaSparkContext();
        sc.getConf().set("es.index.auto.create", "true");
        sc.getConf().set("es.nodes", "localhost:9200");
        //第一行记录
        Map<String, ?> numbers = ImmutableMap.of("agea", 21, "money", 20000);
        //第二行记录
        Map<String, ?> airports = ImmutableMap.of("name", "chy", "addr", "南京");

        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(numbers, airports));

        //索引名称无限制，索引名称小写
        JavaEsSpark.saveToEs(javaRDD, "testsparkjavardd/docs");
    }

    public  static void test2() {
        JavaSparkContext sc = SparkUtil.getJavaSparkContext();
        sc.getConf().set("es.index.auto.create", "true");
        sc.getConf().set("es.nodes", "localhost:9200");
        //第一行记录
        Map<String, ?> numbers = ImmutableMap.of("id", 1001, "agea", 21, "money", 20000);
        //第二行记录
        Map<String, ?> airports = ImmutableMap.of("id", 1002, "name", "zjj", "addr", "合肥");

        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(numbers, airports));

        //指定文档id
        Map<String, String> mapid = ImmutableMap.of("es.mapping.id", "id");

        //索引名称无限制
        JavaEsSpark.saveToEs(javaRDD, "testsparkjavardd/docs", mapid);
    }

    public  static void test3() {
        JavaSparkContext sc = SparkUtil.getJavaSparkContext();
        sc.getConf().set("es.index.auto.create", "true");
        sc.getConf().set("es.nodes", "localhost:9200");


        EsBean p1 = new EsBean("1003","OTP", 21);
        EsBean p2 = new EsBean("1004","MUC", 30);

        JavaRDD<EsBean> javaRDD = sc.parallelize(ImmutableList.of(p1, p2));

        //指定文档id
        //如果id如果动态映射成数字型后，id的值就不能乱输入
        Map<String, String> mapid = ImmutableMap.of("es.mapping.id", "id");

        //索引名称无限制
        JavaEsSpark.saveToEs(javaRDD, "testsparkjavardd/docs", mapid);
    }

    public static void test4(){
        JavaSparkContext sc = SparkUtil.getJavaSparkContext();
        sc.getConf().set("es.index.auto.create", "true");
        sc.getConf().set("es.nodes", "localhost:9200");

        String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\",\"id\" : \"3003\"}";
        String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\",\"id\" : \"3004\"}";

        JavaRDD<String> javaRDD = sc.parallelize(ImmutableList.of(json1, json2));

        //指定文档id
        //如果id如果动态映射成数字型后，id的值就不能乱输入
        Map<String, String> mapid = ImmutableMap.of("es.mapping.id", "id");

        //索引名称无限制
        JavaEsSpark.saveJsonToEs(javaRDD, "testsparkjson/docs",mapid);
    }

    public static class EsBean implements Serializable {
        private String id,name;
        private int age;

        public EsBean(String id,String name, int age) {
            setId(id);
            setName(name);
            setAge(age);
        }

        public EsBean() {}

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
