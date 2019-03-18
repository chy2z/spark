package cn.suning.spark.test.hdfs;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @program: scalaspark
 * @description:  join 操作
 * @author: 18093941
 * @create: 2019-03-08 11:48
 **/
public class JRddJoinJob {
    private static Logger logger = Logger.getLogger(JHdfsJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;

    public static void main(String[] args) throws Exception {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JRddJoinJob.class.getName(), args, 4);
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        String outputPath =hdfsBaseUrl+"/logs/CompareResult";

        if(HadoopHdfsUtil.checkHdfsSuccess(fileSystem,outputPath)) {
            HadoopHdfsUtil.deleteHdfsDir(fileSystem, outputPath);
        }

        JavaRDD<String> oldRdd = javaSparkContext.parallelize(Arrays.asList("手机:hw","电脑:sx","洗衣机:ll","洗衣机:xmz","电冰箱:xx","空调:gl"));
        JavaPairRDD<String,String> pairRDDOld = oldRdd.mapToPair(x -> {
            String[] array = x.split(":");
            return new Tuple2(array[0],array[1]);
        });

        JavaRDD<String> newRdd = javaSparkContext.parallelize(Arrays.asList("电冰箱:ss","手机:xm","电脑:sx","洗衣机:haier","洗衣机:lx","电视:lg"));
        JavaPairRDD<String,String> pairRDDNew = newRdd.mapToPair(x -> {
            String[] array = x.split(":");
            return new Tuple2(array[0],array[1]);
        });


        //JavaRDD<String> result=fullJoin(pairRDDNew,pairRDDOld);

        //JavaRDD<String> result=join(pairRDDNew,pairRDDOld);

        //JavaRDD<String> result=leftOuterJoin(pairRDDNew,pairRDDOld);

        JavaRDD<String> result=rightOuterJoin(pairRDDNew,pairRDDOld);

        /*  从内存数据集创建pairRDD
        List<Tuple2<String,Integer>> lt = new ArrayList<>();
        Tuple2<String,Integer> tp1 = new Tuple2<>("suning1", 2);
        Tuple2<String,Integer> tp2 = new Tuple2<>("suning2", 6);
        Tuple2<String,Integer> tp3 = new Tuple2<>("suning3", 5);
        lt.add(tp1);
        lt.add(tp2);
        lt.add(tp3);
        JavaPairRDD<String,Integer> data = javaSparkContext.parallelizePairs(lt);
        */

        long compareNum = result.count();
        logger.info("数据" + compareNum);
        result.repartition(1).saveAsTextFile(outputPath);

        HadoopHdfsUtil.closeFileSystem(fileSystem);
        closeJavaSparkContext(javaSparkContext);
        closeSparkSession(sparkSession);
    }


    public static SparkConf initSparkConf(String className, String[] args, int length)
    {
        SparkConf sparkConf = new SparkConf().setAppName(className);
        if (PropertyUtil.RunEnvFlagEnum.DEV == PropertyUtil.getRunEnvFlag())
            sparkConf.setMaster("local[1]");
        else {
            sparkConf.setMaster("yarn");
        }
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "cn.suning.spark.test.hdfs.MyRegistrator");
        sparkConf.set("spark.shuffle.consolidateFiles", "true");
        if (PropertyUtil.RunEnvFlagEnum.PRD == PropertyUtil.getRunEnvFlag()) {
            sparkConf.set("spark.default.parallelism", "900");
            sparkConf.set("spark.sql.shuffle.partitions", "900");
        }
        else{
            sparkConf.set("spark.default.parallelism", "200");
            sparkConf.set("spark.sql.shuffle.partitions", "200");
        }
        return sparkConf;
    }

    public static void closeJavaSparkContext(JavaSparkContext javaSparkContext)
    {
        if (null != javaSparkContext) {
            javaSparkContext.stop();
            javaSparkContext.close();
        }
    }

    public static void closeSparkSession(SparkSession sparkSession) {
        if (null != sparkSession) {
            sparkSession.stop();
            sparkSession.close();
        }
    }

    /**
     * 全连接 并集 适合计算不同版本的差异
     * (3,4),(3,5)
     * (1,3),(3,9)
     * (1,(3,null)) (3,(4,9)) (3,(5,9))
     * @param pairRDDNew
     * @param pairRDDOld
     * @return
     */
    public static JavaRDD<String> fullJoin(JavaPairRDD<String,String> pairRDDNew,JavaPairRDD<String,String> pairRDDOld){
        //全连接
        JavaPairRDD tuple2JavaRDD= pairRDDNew.fullOuterJoin(pairRDDOld);

        return tuple2JavaRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>>,String>() {
            @Override
            public Iterator<String> call(Iterator<Tuple2<String, Tuple2<Optional<String>, Optional<String>>>> t) throws Exception {
                List list = new ArrayList();
                while(t.hasNext()) {
                    Tuple2 tuple = (Tuple2) t.next();
                    String key = (String) tuple._1;
                    Tuple2 subs = (Tuple2) tuple._2;
                    Optional oldOpt = (Optional) subs._2;
                    Optional newOpt = (Optional) subs._1;
                    String oldVal = "";
                    if (oldOpt.isPresent()) {
                        oldVal = (String)oldOpt.get();
                    }
                    String newVal = "";
                    if (newOpt.isPresent()) {
                        newVal = (String)newOpt.get();
                    }
                    if ("null".equals(newVal)) {
                        newVal = "";
                    }
                    if ("null".equals(oldVal)) {
                        oldVal = "";
                    }
                    if (StringUtils.isBlank(oldVal)) {
                        list.add(key + "\t" + "新版本=" + newVal + "\t旧版本=" + oldVal);
                    }
                    else if (StringUtils.isBlank(newVal)) {
                        list.add(key + "\t" + "新版本=" + newVal + "\t旧版本=" + oldVal);
                    }
                    else if (!newVal.equals(oldVal))
                    {
                        list.add(key + "\t" + "新版本=" + newVal + "\t旧版本=" + oldVal);
                    }
                }
                return list.iterator();
            }
        });
    }

    /**
     *  内连接 交集 适合计算前后两次都包含
     * (3,4),(3,5)
     * (1,3),(3,9)
     * (3,(4,9)) (3,(5,9))
     * @param pairRDDNew
     * @param pairRDDOld
     * @return
     */
    public static JavaRDD<String> join(JavaPairRDD<String,String> pairRDDNew,JavaPairRDD<String,String> pairRDDOld){
        JavaPairRDD javaPairRDD=pairRDDNew.join(pairRDDOld);

        return javaPairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Tuple2<String,String>>>,String>() {
            @Override
            public Iterator<String> call(Iterator<Tuple2<String,Tuple2<String,String>>> t) throws Exception {
                List<String> list = new ArrayList();
                while (t.hasNext()){
                    Tuple2 tuple = (Tuple2) t.next();
                    String key = (String) tuple._1;
                    Tuple2 subs = (Tuple2) tuple._2;
                    String oldVal = (String)subs._1;
                    String newVal =(String) subs._2;
                    list.add(key + "\t" + "新版本=" + newVal + "\t旧版本=" + oldVal);
                }
                return list.iterator();
            }
        });
    }

    /**
     *  左连接
     * (3,4),(3,5),(4,9)
     * (1,3),(3,9)
     * (1,(3,null)) (3,(4,9)) (3,(5,9))
     * @param pairRDDNew
     * @param pairRDDOld
     * @return
     */
    public static JavaRDD<String> leftOuterJoin(JavaPairRDD<String,String> pairRDDNew,JavaPairRDD<String,String> pairRDDOld){
        JavaPairRDD javaPairRDD=pairRDDNew.leftOuterJoin(pairRDDOld);

        return javaPairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Optional<String>>>>,String>() {
            @Override
            public Iterator<String> call(Iterator<Tuple2<String,Tuple2<String,Optional<String>>>> t) throws Exception {
                List<String> list = new ArrayList();
                while (t.hasNext()){
                    Tuple2 tuple = (Tuple2) t.next();
                    String key = (String) tuple._1;
                    Tuple2 subs = (Tuple2) tuple._2;
                    String oldVal = (String)subs._1;
                    Optional newOpt =(Optional) subs._2;
                    String newVal="";
                    if (newOpt.isPresent()) {
                        newVal = (String)newOpt.get();
                    }
                    if ("null".equals(newVal)) {
                        newVal = "";
                    }
                    if ("null".equals(oldVal)) {
                        oldVal = "";
                    }
                    list.add(key + "\t" + "left=" + newVal + "\tright=" + oldVal);
                }
                return list.iterator();
            }
        });
    }

    /**
     *  右连接
     * (3,4),(3,5),(4,9)
     * (1,3),(3,9)
     * (3,(4,9)) (3,(5,9)),(4,(9,null))
     * @param pairRDDNew
     * @param pairRDDOld
     * @return
     */
    public static JavaRDD<String> rightOuterJoin(JavaPairRDD<String,String> pairRDDNew,JavaPairRDD<String,String> pairRDDOld){
        JavaPairRDD javaPairRDD=pairRDDNew.rightOuterJoin(pairRDDOld);

        return javaPairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Tuple2<Optional<String>,String>>>,String>() {
            @Override
            public Iterator<String> call(Iterator<Tuple2<String,Tuple2<Optional<String>,String>>> t) throws Exception {
                List<String> list = new ArrayList();
                while (t.hasNext()){
                    Tuple2 tuple = (Tuple2) t.next();
                    String key = (String) tuple._1;
                    Tuple2 subs = (Tuple2) tuple._2;
                    String oldVal ="";
                    Optional oldOpt =(Optional) subs._1;
                    String newVal=(String)subs._2;
                    if (oldOpt.isPresent()) {
                        oldVal = (String)oldOpt.get();
                    }
                    if ("null".equals(newVal)) {
                        newVal = "";
                    }
                    if ("null".equals(oldVal)) {
                        oldVal = "";
                    }
                    list.add(key + "\t" + "left=" + newVal + "\tright=" + oldVal);
                }
                return list.iterator();
            }
        });
    }

}
