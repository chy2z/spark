package com.chy.sql;

import com.chy.util.SparkUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
* @Title: temporaryViews
* @Description: 全局临时视图
* @author chy
* @date 2018/5/18 15:11
*/
public class temporaryViews {
    public static void main(String[] args){

        SparkSession spark= SparkUtil.getSparkSession();

        Dataset<Row> df = spark.read().json("src/main/resources/people.json");

        //显示所有数据
        df.show();

        // 注册全局临时视图
        try {
            df.createGlobalTempView("people");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        // 查询全局临时视图
        spark.sql("SELECT * FROM global_temp.people").show();

        // 会话全局视图
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
    }
}
