package com.chy.rdd;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

/**
* @Title: accumulator
* @Description: 累加器
* @author chy
* @date 2018/5/16 15:03
*/
public class accumulator {

    public static void main(String[] args){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();

        /**
         * 累加器
         */
        LongAccumulator accum = sc.sc().longAccumulator();

        /**
         * 自定义累加器
         */
        MyAccumulator myAccumulator=new MyAccumulator();

        /**
         * 注册累加器
         */
        sc.sc().register(myAccumulator,"MyAccumulator");

        JavaRDD<Integer> distData= sc.parallelize(Arrays.asList(1, 2, 3, 4));

        JavaRDD<Integer> rdd = distData.map((s) ->{
            accum.add(s+1);
            myAccumulator.add(Long.parseLong(String.valueOf(s+1)));
            return  s+1;
        });

        /**
         * 缓存
         * map执行一遍
         */
        rdd.cache();

        rdd.count();
        rdd.count();

        /**
         * 缓存:14
         * 不缓存:28
         */
        System.out.println(accum.value());

        System.out.println(myAccumulator.value());
    }


    /**
     * 自定义累加器
     */
    public static class MyAccumulator extends AccumulatorV2<Long,Long> {

        private long my = 0L;

        @Override
        public void reset() {
            my = 0L;
        }

        @Override
        public void add(Long l) {
            my += l;
        }

        @Override
        public AccumulatorV2<Long, Long> copy() {
            MyAccumulator copy = new MyAccumulator();
            copy.add(my);
            return copy;
        }

        @Override
        public void merge(AccumulatorV2<Long, Long> other) {
            my += other.value();
        }

        @Override
        public boolean isZero() {
            return my == 0L;
        }

        @Override
        public Long value() {
            return my;
        }
    }
}
