package com.chy.rdd.transformation;

import com.chy.util.SparkUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
* @Title: sparkAggregateByKey
* @Description: aggregateByKey 算子

* @author chy
* @date 2018/5/17 16:20
*/
public class sparkAggregateByKey {

    public static void main(String[] arg){
        JavaSparkContext sc= SparkUtil.getJavaSparkContext();
        List<String> list = Arrays.asList("you,jump", "he,jump","he");
        JavaRDD<String> listRDD = sc.parallelize(list);

        /**
         * flatMap 拆分元素
         */
        listRDD.flatMap(line -> Arrays.asList(line.split(",")).iterator())

                /**
                 * 形成 k,v
                 */
                .mapToPair(word -> new Tuple2<>(word,1))

                /**
                 *  （you，1）
                 *  （jump，1）
                 *   (he,1)
                 *   (jump,1)
                 *   (he,1)
                 *   -----seqFunc-----
                 *   （you，（1，zeroValue））--- （you，（1，1））---(x+y)--（you，（2）
                 *    ------------------------- （jump，（1，1））--(x+y)--（jump，（2）
                 *    ------------------------- （he，（1，1））-----(x+y)--（he，（2）
                 *    ------------------------- （jump，（1，1））--(x+y)--（jump，（2）
                 *    ------------------------- （he，（1，1））--(x+y)--（he，（2）
                 *
                 *    --------combFunc-----
                 *    ------------------------- （jump，2）+（jump，2）= （jump，（2+2=4))
                 *    ------------------------- （he，2）+（he，2）=（he，（2+2)=4))
                 *
                 *    --------result-----------
                 *    -------------------------（you，（2）
                 *    -------------------------（jump，（4）
                 *    -------------------------（he，（4）
                 */
                .aggregateByKey(1,(x,y)->{
                    System.out.println("x:"+x+",y:"+y);
                   return x+y;
                } ,(m,n) ->{
                    //有多个的情况执行联合合并
                    System.out.println("m:"+m+",n:"+n);
                    return m+n;
                })
                .foreach(tuple -> System.out.println(tuple._1+"->"+tuple._2));
    }

}
