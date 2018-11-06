package com.chy.sql.datasources;

import com.chy.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
* @Title: redJdbc
* @Description: 读取jdbc 数据源
* @author chy
* @date 2018/11/6 22:22
*/
public class redJdbc {

    public static void main(String[] arg){








        test3();


    }

    public static void test3(){
        String url="jdbc:mysql://localhost:3306/sparksql?user=root&password=wmzycn";
        String tableName="users";
        SparkSession spark= SparkUtil.getSparkSession();

        Map<String,String> map=new HashMap<>();
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("url",url);
        map.put("dbtable",tableName);
        map.put("fetchSize","100");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "wmzycn");
        connectionProperties.put("isolationLevel","REPEATABLE_READ");

        //方式1：读取users信息
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .options(map)
                .load();

        //保存到users2表
        jdbcDF.write()
               .option("createTableColumnTypes", "  `id` int, `name` varchar(30) ")
               .jdbc(url, "users4", connectionProperties);
    }

    public static void test2(){

        String url="jdbc:mysql://localhost:3306/sparksql?user=root&password=wmzycn";
        String tempTableName=" (select id,name from users) as u";
        SparkSession spark= SparkUtil.getSparkSession();


        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "wmzycn");
        connectionProperties.put("isolationLevel","REPEATABLE_READ");


        //方式2：读取users信息
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc(url, tempTableName, connectionProperties);

        //保存到users3表
        jdbcDF2.write()
                .jdbc(url, "users3", connectionProperties);


    }

    public static void test1(){
        String url="jdbc:mysql://localhost:3306/sparksql?user=root&password=wmzycn";

        String tableName="users";

        SparkSession spark= SparkUtil.getSparkSession();

        Map<String,String> map=new HashMap<>();
        map.put("driver","com.mysql.jdbc.Driver");
        map.put("url",url);
        map.put("dbtable",tableName);
        map.put("fetchSize","100");


        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "wmzycn");
        connectionProperties.put("isolationLevel","REPEATABLE_READ");

        //方式1：读取users信息
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .options(map)
                .load();


        //保存到users_copy表
        jdbcDF.write()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "users1")
                .save();

        jdbcDF.select("id", "name").write().format("parquet").save("src/main/resources/temp/jdbc_users_parquet");

        jdbcDF.select("id", "name").write().format("json").save("src/main/resources/temp/jdbc_users_json");

        //存储为新表
        jdbcDF.select("id", "name").write().mode("overwrite").saveAsTable("users_copy");


        jdbcDF.createOrReplaceTempView("usersview");

        Dataset<Row> jdbcSQl = spark.sql("select * from usersview where name like '1%' ");

        jdbcSQl.show();

        jdbcSQl.write().format("json").save("src/main/resources/temp/jdbc_users_j");
    }
}
