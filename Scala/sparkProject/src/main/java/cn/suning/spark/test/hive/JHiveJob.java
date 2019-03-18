package cn.suning.spark.test.hive;


import org.apache.spark.sql.SparkSession;

/**
 * @program: scalaspark
 * @description:
 * @author: 18093941
 * @create: 2019-03-05 20:41
 **/
public class JHiveJob {

    public static SparkSession getSparkSessionForHive() {
        return SparkSession
                .builder()
                .appName("JHiveJob")
                .master("local[*]")
                .enableHiveSupport().getOrCreate();

    }

    public static void main(String[] args) {
        SparkSession spark = getSparkSessionForHive();
        spark.sql("show tables").show();
        spark.sql("select * from person").show();

        spark.close();
    }

}
