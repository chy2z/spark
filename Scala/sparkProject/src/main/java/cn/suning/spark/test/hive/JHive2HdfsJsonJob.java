package cn.suning.spark.test.hive;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import cn.suning.spark.test.hdfs.JHdfsJob;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;



/**
 * @program: scalaspark
 * @description:
 * @author: 18093941
 * @create: 2019-03-09 14:39
 **/
public class JHive2HdfsJsonJob {
    private static Logger logger = Logger.getLogger(JHdfsJob.class);
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static String hdfsBaseUrl;

    public static void main(String[] args) throws Exception {
        PropertyUtil.setCommFlags("dev,");
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        fileSystem = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
        SparkConf sparkConf = initSparkConf(JHive2HdfsCVSJob.class.getName(), args, 4);
        //读取hive enableHiveSupport
        sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        //输出目录
        String outputPath = hdfsBaseUrl + "/logs/HiveResult";

        //读取数据
        Dataset xpDataFrame = sparkSession.sql("select * from person");
        xpDataFrame.printSchema();
        xpDataFrame.show(10);
        JavaRDD<Row> rowRdd = xpDataFrame.toJavaRDD();
        JavaPairRDD mainRdd = rowRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, String>() {
            public Iterator<Tuple2<String, String>> call(Iterator<Row> rowIterator) throws Exception {
                List list = new ArrayList();
                while (rowIterator.hasNext()) {
                    Row row = (Row) rowIterator.next();
                    if (row == null) {
                        return null;
                    }
                    Person person=new Person();
                    person.setName(row.getString(0));
                    person.setAge(row.getInt(1));
                    person.setSchool(row.getString(2));
                    person.setAddress(row.getString(3));
                    Tuple2 t2 = new Tuple2(row.getString(0), JSONObject.toJSONString(person));
                    list.add(t2);
                }
                return list.iterator();
            }
        });

        HadoopHdfsUtil.deleteHdfsDir(fileSystem,outputPath);

        /**
         * 按照不同的key生成key.json文件
         */
        mainRdd.partitionBy(new HashPartitioner(2)).saveAsHadoopFile(outputPath, String.class, String.class, RDDMultipleTextOutputFormatJson.class);

        HadoopHdfsUtil.closeFileSystem(fileSystem);
        closeJavaSparkContext(javaSparkContext);
        closeSparkSession(sparkSession);
    }

    public static SparkConf initSparkConf(String className, String[] args, int length) {
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
        } else {
            sparkConf.set("spark.default.parallelism", "200");
            sparkConf.set("spark.sql.shuffle.partitions", "200");
        }
        return sparkConf;
    }

    public static void closeJavaSparkContext(JavaSparkContext javaSparkContext) {
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

    protected static class RDDMultipleTextOutputFormatJson extends JHive2HdfsCVSJob.MultipleOnlyValueOutputFormat<String, String> {
        public String generateFileNameForKeyValue(String key, String value, String name)
        {
            return key + ".json";
        }
    }

    protected static class MultipleOnlyValueOutputFormat<K, V> extends MultipleOutputFormat<K, V> {
        private JHive2HdfsCVSJob.OnlyValueOutputFormat<K, V> theTextOutputFormat = null;

        protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3)
                throws IOException {
            if (this.theTextOutputFormat == null) {
                this.theTextOutputFormat = new JHive2HdfsCVSJob.OnlyValueOutputFormat();
            }
            return this.theTextOutputFormat.getRecordWriter(fs, job, name, arg3);
        }
    }

    protected static class OnlyValueOutputFormat<K, V> extends FileOutputFormat<K, V> {
        public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
                throws IOException {
            boolean isCompressed = getCompressOutput(job);
            String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "\t");

            if (!isCompressed) {
                Path file = FileOutputFormat.getTaskOutputPath(job, name);
                FileSystem fs = file.getFileSystem(job);
                FSDataOutputStream fileOut = fs.create(file, progress);
                return new JHive2HdfsCVSJob.OnlyValueOutputFormat.LineRecordWriter(fileOut, keyValueSeparator);
            }
            Class codecClass = getOutputCompressorClass(job, GzipCodec.class);

            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);

            Path file = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension());

            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new JHive2HdfsCVSJob.OnlyValueOutputFormat.LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
        }

        protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
            private static final byte[] newline;
            protected DataOutputStream out;
            private final byte[] keyValueSeparator;
            private static final String utf8 = "UTF-8";

            public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
                this.out = out;
                try {
                    this.keyValueSeparator = keyValueSeparator.getBytes("UTF-8");
                } catch (UnsupportedEncodingException uee) {
                    throw new IllegalArgumentException("can't find UTF-8 encoding");
                }
            }

            public LineRecordWriter(DataOutputStream out) {
                this(out, "\t");
            }

            private void writeObject(Object o)
                    throws IOException {
                if ((o instanceof Text)) {
                    Text to = (Text) o;
                    this.out.write(to.getBytes(), 0, to.getLength());
                } else {
                    this.out.write(o.toString().getBytes(utf8));
                }
            }

            public synchronized void write(K key, V value)
                    throws IOException {
                boolean nullKey = (key == null) || ((key instanceof NullWritable));
                boolean nullValue = (value == null) || ((value instanceof NullWritable));
                if ((nullKey) && (nullValue)) {
                    return;
                }

                if (!nullValue) {
                    writeObject(value);
                }
                this.out.write(newline);
            }

            public synchronized void close(Reporter reporter) throws IOException {
                this.out.close();
            }

            static {
                try {
                    newline = "\n".getBytes(utf8);
                } catch (UnsupportedEncodingException uee) {
                    throw new IllegalArgumentException("can't find UTF-8 encoding");
                }
            }
        }
    }

    protected static class Person {
        String name;
        Integer age;
        String address;
        String school;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getSchool() {
            return school;
        }

        public void setSchool(String school) {
            this.school = school;
        }
    }
}
