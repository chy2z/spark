package cn.suning.hadoop.job.job1;

import java.io.IOException;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class SimpleJob {

    public static Job getJobInstance(String[] args)  throws IOException {
        Configuration conf = new Configuration();
        conf.set("env", "dev,");
        int reduceTaskSize = 2;

        Job wcjob = Job.getInstance(conf);

        wcjob.setJarByClass(GoodsAttrMain.class);

        wcjob.setMapperClass(DataMapper.class);

        wcjob.setReducerClass(DataReduce.class);

        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(Text.class);
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(Text.class);
        wcjob.setNumReduceTasks(reduceTaskSize);
        return wcjob;
    }

    public static FileSystem getFileSystem(String[] args) {
        PropertyUtil.setCommFlags("dev,");
        String hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        return HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
    }

}
