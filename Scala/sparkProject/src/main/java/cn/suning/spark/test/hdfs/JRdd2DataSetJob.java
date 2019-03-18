package cn.suning.spark.test.hdfs;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * @program: scalaspark
 * @description:
 * @author: 18093941
 * @create: 2019-03-07 19:28
 **/
public class JRdd2DataSetJob {
    private static Logger logger = Logger.getLogger(JHdfsJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;

    public static void main(String[] args) throws Exception {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JRdd2DataSetJob.class.getName(), args, 4);
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        /**
         * 路径
         * 数据格式:chy,21,搜索,南京
         */
        String inPath = "/rdd/rdd2dataset.txt";
        //数据分隔符
        String separator=",";
        //列信息
        String columns="name:varchar,age:integer,type:varchar,addr:varchar";

        //判断文件是否写入成功
        boolean flag= HadoopHdfsUtil.checkFileExists(fileSystem,hdfsBaseUrl+inPath);

        if(flag) {
            JavaRDD<String> rdd = javaSparkContext.textFile(hdfsBaseUrl+inPath);
            rdd.foreach((s)-> {
                logger.info("内容---->"+s);
            });

            Dataset<Row> dataset=javaRDD2Dataset(sparkSession,rdd,separator,columns);

            dataset.printSchema();

            dataset.show(10);
        }
        else{
            logger.info("文件不存在");
        }

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
        sparkConf.set("spark.default.parallelism", "600");
        sparkConf.set("spark.sql.shuffle.partitions", "600");
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

    public static Date parseString2Date(String str) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse(str);
        } catch (Exception e) {
            logger.error("时间转换报错." + e.getMessage());
        }
        return date;
    }

    /**
     * DataRow 结构
     * @param colNameAndTypes
     * @return
     */
    public static StructType createStructType(String[] colNameAndTypes)
    {
        if ((null == colNameAndTypes) || (colNameAndTypes.length == 0)) {
            return null;
        }
        List structFields = new ArrayList();
        for (String colNameAndType : colNameAndTypes) {
            String[] vals = colNameAndType.split(":");
            if (1 == vals.length) {
                structFields.add(DataTypes.createStructField(vals[0], DataTypes.StringType, true));
            } else if (2 == vals.length) {
                String colName = vals[0];
                String colType = vals[1];
                if ("VARCHAR".equals(colType.toUpperCase()))
                    structFields.add(DataTypes.createStructField(colName, DataTypes.StringType, true));
                else if ("TIMESTAMP".equals(colType.toUpperCase()))
                    structFields.add(DataTypes.createStructField(colName, DataTypes.TimestampType, true));
                else if ("DOUBLE".equals(colType.toUpperCase()))
                    structFields.add(DataTypes.createStructField(colName, DataTypes.DoubleType, true));
                else if ("INTEGER".equals(colType.toUpperCase()))
                    structFields.add(DataTypes.createStructField(colName, DataTypes.IntegerType, true));
                else if ("DATE".equals(colType.toUpperCase())) {
                    structFields.add(DataTypes.createStructField(colName, DataTypes.DateType, true));
                }
            }
        }
        StructType structType = DataTypes.createStructType(structFields);
        return structType;
    }

    public static Object getTypeData(String colNameAndType,String value) {
        String[] vals = colNameAndType.split(":");
        String colName = vals[0];
        String colType = vals[1];
        if ("VARCHAR".equals(colType.toUpperCase()))
            return value;
        else if ("TIMESTAMP".equals(colType.toUpperCase()))
            return parseString2Date(value).getTime();
        else if ("DOUBLE".equals(colType.toUpperCase()))
            return Double.parseDouble(value);
        else if ("INTEGER".equals(colType.toUpperCase()))
            return Integer.parseInt(value);
        else if ("DATE".equals(colType.toUpperCase())) {
            return parseString2Date(value);
        }
        return null;
    }

    /**
     * rdd 2 dataset 转换方法
     * @param sparkSession
     * @param javaRDD
     * @param separator
     * @param columns
     * @return
     */
    public static Dataset<Row> javaRDD2Dataset(SparkSession sparkSession, JavaRDD<String> javaRDD, String separator, String columns) {
        String[] cols = columns.replaceAll(" ", "").split(",", -1);
        int colsLen = cols.length;
        JavaRDD rowJavaRDD = javaRDD.mapPartitions(new RowFlatMapFunction(separator, cols));
        Dataset dataset = sparkSession.createDataFrame(rowJavaRDD, createStructType(cols));
        return dataset;
    }

    /**
     * 返回 JavaRdd Row
      */
    static class RowFlatMapFunction implements FlatMapFunction<Iterator<String>, Row> {
        private String separator = "\t";
        private int colsLen;
        private String[] colNameAndTypes;

        public RowFlatMapFunction(String separator, String[] colNameAndTypes) {
            this.separator = separator;
            this.colsLen = colNameAndTypes.length;
            this.colNameAndTypes=colNameAndTypes;
        }

        public Iterator<Row> call(Iterator<String> t) throws Exception {
            List list = new ArrayList();
            while (t.hasNext()) {
                String line = (String) t.next();
                String[] vals = line.split(this.separator, -1);
                int len = vals.length;
                if (len >= this.colsLen) {
                    Object[] objs = new Object[this.colsLen];
                    for (int i = 0; i < this.colsLen; i++) {
                        objs[i] = getTypeData(this.colNameAndTypes[i],vals[i]);
                    }
                    list.add(RowFactory.create(objs));
                }
            }
            return list.iterator();
        }
    }
}
