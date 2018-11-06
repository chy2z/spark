package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkReduce
* @Description: Reduce 算子
  将RDD中的所有元素进行合并，每次合并成1个新元素，并新元素传入，在和下一个元素继续合并。
* @author chy
* @date 2018/5/17 10:08
*/
public class sparkReduce {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        Integer[] names = {1,2,3,4,5,6};
        List<Integer> list = Arrays.asList(names);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        Integer sum= listRDD.reduce((x,y) -> {
            System.out.println(x+"+"+y+"="+(x+y));
            return x+y;
        });
        System.out.println("结果:"+sum);
    }

}
