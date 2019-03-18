package cn.suning.hadoop.job.job1;

import java.text.SimpleDateFormat;
import java.util.Date;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.HdfsUtils;
import cn.suning.hadoop.PropertyUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class GoodsAttrMain
{
    private static Logger logger = Logger.getLogger(GoodsAttrMain.class);

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            logger.info("arg[" + i + "]:" + args[i]);
        }

        FileSystem fs = SimpleJob.getFileSystem(args);
        Job wcjob = SimpleJob.getJobInstance(args);

        //jar 入口
        wcjob.setJarByClass(GoodsAttrMain.class);
        wcjob.setMapperClass(DataMapper.class);
        wcjob.setReducerClass(DataReduce.class);

        String gpPath = "/user/sousuo/data/das/common/sparkGoodsAttr/goodsAttrParamCollect/full/";
        String ggPath = "/user/sousuo/data/das/common/sparkGoodsAttr/goodsAttrGsaleCatalogCollect/full/";
        logger.info("fs:" + fs);
        Path path1 = new Path(HdfsUtils.getAllPath(fs, gpPath, 4));
        Path path2 = new Path(HdfsUtils.getAllPath(fs, ggPath, 4));
        FileInputFormat.setInputPaths(wcjob, new Path[] { path1, path2 });
        SimpleDateFormat fdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat ftf = new SimpleDateFormat("HHmmss");

        Path path = new Path("/user/sousuo/data/das/hadoop/attr/part1/" + fdf.format(new Date()) + "/" + ftf.format(new Date()));
        FileOutputFormat.setOutputPath(wcjob, path);

        wcjob.waitForCompletion(true);
    }
}