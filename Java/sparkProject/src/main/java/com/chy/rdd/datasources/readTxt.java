package com.chy.rdd.datasources;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
* @Title: readTxt
* @Description: 读取文本内容
* @author chy
* @date 2018/5/16 10:31
*/
public class readTxt {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();

        /**
         * 项目路径
         */
        JavaRDD<String> lines = sc.textFile("src/main/resources/data.txt");

        /**
         * 绝对路径
         */
        //JavaRDD<String> lines = sc.textFile("e:\\data.txt");

        /**
         * Transformations 算子
         * map 算子 返回新的RDD
         * 不会立即执行
         */
        JavaRDD<Integer> lineLengths = lines.map((s) ->{
            System.out.println(s+":"+s.length());
            return  s.length();
        } );

        /**
         * Actions 算子
         * 计算总长度
         * 立即执行
         */
        int totalLength = lineLengths.reduce((a, b) ->{
            System.out.println("a:"+a);
            System.out.println("b:"+b);
            return  a + b;
        } );

        /**
         * 保存结果
         */
        lineLengths.saveAsTextFile("E:\\result");

        System.out.println(totalLength);
    }

}
