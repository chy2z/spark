package cn.suning.spark.demo1.util;

import cn.suning.hadoop.PropertyUtil;
import cn.suning.spark.demo1.function.DpsRowByIndexsFlatMapFunction;
import cn.suning.spark.demo1.function.DpsRowFlatMapFunction;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SparkUtil
{
    private static Logger logger = Logger.getLogger(SparkUtil.class);

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
            logger.info("close JavaSparkContext");
        }
    }

    public static void closeSparkSession(SparkSession sparkSession) {
        if (null != sparkSession) {
            sparkSession.stop();
            sparkSession.close();
            logger.info("close SparkSession");
        }
    }

    /**
     * 打印日志
     * @param hdfsBaseUrl
     * @param javaSparkContext
     * @param list
     */
    public static void log(String hdfsBaseUrl, JavaSparkContext javaSparkContext, List<String> list) {
        JavaRDD javaRDD = javaSparkContext.parallelize(list);
        javaRDD.repartition(1).saveAsTextFile(hdfsBaseUrl + "/logs/findBugLog/" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "/" + System.currentTimeMillis());
    }

    /**
     * 注册自定义函数
     * @param sqlContext
     */
    public static void registerMyFuns(SQLContext sqlContext)
    {
        sqlContext.udf().register("dps_len", new UDF1<String,Integer>() {
                    private static final long serialVersionUID = -3267402503751523214L;
                    public Integer call(String str) throws Exception {
                        if (org.apache.commons.lang3.StringUtils.isNotBlank(str)) {
                            return Integer.valueOf(str.length());
                        }
                        return Integer.valueOf(0);
                    }
                }
                , DataTypes.IntegerType);

        sqlContext.udf().register("dps_case", new UDF2<String,String,String>() {
                    private static final long serialVersionUID = -269109534256078474L;

                    public String call(String val_1, String val_2) throws Exception {
                        if (org.apache.commons.lang3.StringUtils.isNoneBlank(new CharSequence[] { val_2 })) {
                            return val_2;
                        }
                        return val_1;
                    }
                }
                , DataTypes.StringType);

        sqlContext.udf().register("defaultFullTime", new UDF1<String,String>() {
                    private static final long serialVersionUID = 1130130747568837309L;

                    public String call(String t1) throws Exception {
                        return "1970-01-01 08:00:00.0";
                    }
                }
                , DataTypes.StringType);

        sqlContext.udf().register("dps_concat", new UDF3<String,String,String,String>() {
                    private static final long serialVersionUID = 2560118600699375883L;

                    public String call(String t1, String t2, String t3) throws Exception {
                        if ("prefix".equals(t1))
                            return t2 + t3;
                        if ("suffix".equals(t1)) {
                            return t3 + t2;
                        }
                        return t2 + t3;
                    }
                }
                , DataTypes.StringType);

        sqlContext.udf().register("dps_to_number",  new UDF1<String,Double>() {
                    private static final long serialVersionUID = 1130130747568837309L;

                    public Double call(String t1) throws Exception {
                        return Double.valueOf(Double.parseDouble(t1));
                    }
                }
                , DataTypes.DoubleType);

        sqlContext.udf().register("my_collect_list", new UserDefinedAggregateFunction()
        {
            private static final long serialVersionUID = 1130130747568837309L;

            public StructType bufferSchema() {
                List structFields = new ArrayList();
                structFields.add(DataTypes.createStructField("sb", DataTypes.StringType, true));
                StructType structType = DataTypes.createStructType(structFields);
                return structType;
            }

            public DataType dataType()
            {
                return DataTypes.StringType;
            }

            public boolean deterministic()
            {
                return true;
            }

            public Object evaluate(Row row)
            {
                return row.getString(0);
            }

            public void initialize(MutableAggregationBuffer buffer)
            {
                buffer.update(0, "");
            }

            public StructType inputSchema()
            {
                List structFields = new ArrayList();

                structFields.add(DataTypes.createStructField("cc", DataTypes.StringType, true));

                structFields.add(DataTypes.createStructField("cs", DataTypes.StringType, true));
                StructType structType = DataTypes.createStructType(structFields);
                return structType;
            }

            public void merge(MutableAggregationBuffer buffer1, Row buffer2)
            {
                buffer1.update(0, buffer1.getString(0) + buffer2.getString(0));
            }

            public void update(MutableAggregationBuffer buffer, Row input)
            {
                buffer.update(0, input.getString(0) + input.getString(1) + buffer.getString(0));
            }
        });
        sqlContext.udf().register("my_collect_set", new UserDefinedAggregateFunction()
        {
            private static final long serialVersionUID = 4733602513225386585L;

            public StructType bufferSchema() {
                List structFields = new ArrayList();
                structFields.add(DataTypes.createStructField("sb", DataTypes.StringType, true));
                StructType structType = DataTypes.createStructType(structFields);
                return structType;
            }

            public DataType dataType()
            {
                return DataTypes.StringType;
            }

            public boolean deterministic()
            {
                return true;
            }

            public Object evaluate(Row row)
            {
                return row.getString(0);
            }

            public void initialize(MutableAggregationBuffer buffer)
            {
                buffer.update(0, "");
            }

            public StructType inputSchema()
            {
                List structFields = new ArrayList();

                structFields.add(DataTypes.createStructField("cc", DataTypes.StringType, true));

                structFields.add(DataTypes.createStructField("cs", DataTypes.StringType, true));
                StructType structType = DataTypes.createStructType(structFields);
                return structType;
            }

            public void merge(MutableAggregationBuffer buffer1, Row buffer2)
            {
                buffer1.update(0, buffer1.getString(0) + buffer2.getString(0));
            }

            public void update(MutableAggregationBuffer buffer, Row input)
            {
                buffer.update(0, input.getString(0) + input.getString(1) + buffer.getString(0));
            }
        });
    }

    public static StructType createStructType(String[] colNameAndTypes) {
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

    public static Dataset<Row> javaRDDToDataFrame(SparkSession sparkSession, JavaRDD<String> javaRDD, String separator, String columns, String indexStrs, String partnumberIndexs, String vendorIndexs, String zipIndexs)
    {
        String[] cols = columns.replaceAll(" ", "").split(",", -1);
        String[] index = indexStrs.replaceAll(" ", "").split(",", -1);
        if (index.length != cols.length) {
            logger.error(columns + " 和 " + indexStrs + " 列和字段索引 个数不一致");
            return null;
        }
        JavaRDD rowJavaRDD = javaRDD.mapPartitions(new DpsRowByIndexsFlatMapFunction(separator, indexStrs, partnumberIndexs, vendorIndexs, zipIndexs));
        Dataset dataFrame = sparkSession.createDataFrame(rowJavaRDD, createStructType(cols));
        return dataFrame;
    }

    public static Dataset<Row> javaRDDToDataset(SparkSession sparkSession, JavaRDD<String> javaRDD, String separator, String columns)
    {
        String[] cols = columns.replaceAll(" ", "").split(",", -1);
        int colsLen = cols.length;
        JavaRDD rowJavaRDD = javaRDD.mapPartitions(new DpsRowFlatMapFunction(separator, colsLen));
        Dataset dataset = sparkSession.createDataFrame(rowJavaRDD, createStructType(cols));
        return dataset;
    }


    public static void main(String[] args)
    {
    }
}