package cn.suning.spark.demo1.util;

import cn.suning.hadoop.DateUtil;
import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.MessageUtil;
import cn.suning.hadoop.PropertyUtil;
import cn.suning.spark.demo1.function.*;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;



public abstract class CommonSparkTemplate implements Serializable
{
    private static final long serialVersionUID = -1647001252884546766L;
    public static Logger logger = Logger.getLogger(CommonSparkTemplate.class);
    public static final int SERVICR_IMS_TIME = 6;
    public static final int SERVICR_DAYTIME_SUC = 5;
    public static final int SERVICR_DAY_TIME_SUC_YESDAY = 4;
    public static final int SERVICR_DAY_TIME_SUC = 3;
    public static final int SERVICR_DAY_TIME = 2;
    public static final int SERVICR_DAY = 1;
    public static final int SERVICR_NONE = 0;
    public static String currentDateStr = DateUtil.getTime(0);

    public static String yesdayDateStr = DateUtil.getTime(-1);

    public static String allStartDateStr = "";

    public static String allStartVersionStr = "";
    public static String hdfsBaseUrl;
    public static String fullOutputPath;
    public static String lastOutputPath;
    public static String incOutputPath;
    public static String outputTimeStr;
    public static boolean flag = false;
    public static boolean isPrd = false;

    public static String hdfsCheckMsg = "";

    public static String compareZkKey = "";

    public static String className = "";
    public static SparkSession sparkSession;
    public static JavaSparkContext javaSparkContext;
    public static FileSystem fileSystem;
    public static final String SIGN_COMMA = ",";
    public static final String SIGN_SEMICOLON = ";";
    public static final String SIGN_AT = "@";
    public static final String SIGN_TAB = "\t";
    public static String PMCS_VERSION = "";

    public static String version = "";

    public static String getClassName(Class<?> myClass)
    {
        String clazzName = myClass.getName();
        className = "DPS_" + clazzName.substring(clazzName.lastIndexOf(46) + 1);
        return className;
    }

    public static String getClassName(String clazzName)
    {
        className = "DPS_" + clazzName.substring(clazzName.lastIndexOf(46) + 1);
        return className;
    }

    public static boolean checkTime(int start, int end)
    {
        String hourStr = new SimpleDateFormat("HH").format(new Date());
        if ((start <= Integer.parseInt(hourStr)) && (Integer.parseInt(hourStr) <= end)) {
            return true;
        }
        return false;
    }

    public static void saveAsTextFile(JavaRDD<String> rdd, String hdfsPath)
    {
        if (isPrd)
            rdd.repartition(100).saveAsTextFile(hdfsPath);
        else
            rdd.repartition(1).saveAsTextFile(hdfsPath);
    }

    public static void saveAsTextFile(JavaRDD<String> rdd, String hdfsPath, int prdNum)
    {
        if (isPrd)
            rdd.repartition(prdNum).saveAsTextFile(hdfsPath);
        else
            rdd.repartition(1).saveAsTextFile(hdfsPath);
    }

    public static void saveAsTextFile(JavaRDD<String> rdd, String hdfsPath, int prdNum, int saveNum, String path)
    {
        if (isPrd)
            rdd.repartition(prdNum).saveAsTextFile(hdfsPath);
        else {
            rdd.repartition(1).saveAsTextFile(hdfsPath);
        }
        HadoopHdfsUtil.clearHdfs(hdfsBaseUrl + path, fileSystem, saveNum);
    }

    public static String saveAsTextFile(JavaRDD<String> rdd, int prdNum, int saveNum, String path, int dayType)
    {
        String outputPath = getOutputHdfsPath(fileSystem, path, dayType);
        if (isPrd)
            rdd.repartition(prdNum).saveAsTextFile(outputPath);
        else {
            rdd.repartition(1).saveAsTextFile(outputPath, GzipCodec.class);
        }
        HadoopHdfsUtil.clearHdfs(hdfsBaseUrl + path, fileSystem, saveNum);
        return outputPath;
    }

    public static String saveAsTextFileNotZip(JavaRDD<String> rdd, int prdNum, int saveNum, String path, int dayType)
    {
        String outputPath = getOutputHdfsPath(fileSystem, path, dayType);
        if (isPrd)
            rdd.repartition(prdNum).saveAsTextFile(outputPath);
        else {
            rdd.repartition(1).saveAsTextFile(outputPath);
        }
        HadoopHdfsUtil.clearHdfs(hdfsBaseUrl + path, fileSystem, saveNum);
        return outputPath;
    }

    public static String saveAsTextFileCoalesce(JavaRDD<String> rdd, int prdNum, int saveNum, String path, int dayType)
    {
        String outputPath = getOutputHdfsPath(fileSystem, path, dayType);
        if (isPrd)
            rdd.coalesce(prdNum).saveAsTextFile(outputPath);
        else {
            rdd.coalesce(10).saveAsTextFile(outputPath, GzipCodec.class);
        }
        HadoopHdfsUtil.clearHdfs(hdfsBaseUrl + path, fileSystem, saveNum);
        return outputPath;
    }

    public static String saveAsTextFileCoalesce(JavaRDD<String> rdd, int prdNum, int saveNum, String path, int dayType, int preNum)
    {
        String outputPath = getOutputHdfsPath(fileSystem, path, dayType);
        if (isPrd)
            rdd.coalesce(prdNum).saveAsTextFile(outputPath);
        else {
            rdd.coalesce(preNum).saveAsTextFile(outputPath, GzipCodec.class);
        }
        HadoopHdfsUtil.clearHdfs(hdfsBaseUrl + path, fileSystem, saveNum);
        return outputPath;
    }

    public static String saveAsTextFile(JavaRDD<String> rdd, int saveNum, String path, int dayType)
    {
        String outputPath = getOutputHdfsPath(fileSystem, path, dayType);
        if (isPrd)
            rdd.saveAsTextFile(outputPath);
        else {
            rdd.coalesce(1).saveAsTextFile(outputPath, GzipCodec.class);
        }
        HadoopHdfsUtil.clearHdfs(hdfsBaseUrl + path, fileSystem, saveNum);
        return outputPath;
    }

    public static boolean checkTime(Integer[] times)
    {
        String hourStr = new SimpleDateFormat("HH").format(new Date());
        boolean flag = false;
        Integer[] arr$ = times; int len$ = arr$.length; for (int i$ = 0; i$ < len$; i$++) { int t = arr$[i$].intValue();
        if (Integer.parseInt(hourStr) == t) {
            flag = true;
        }
    }
        return flag;
    }

    public static boolean checkHdfs(FileSystem fileSystem, String[] path1)
    {
        boolean flag = true;
        for (String path : path1) {
            if ((StringUtils.isEmpty(path)) || (path.contains("[error]"))) {
                hdfsCheckMsg = hdfsCheckMsg + ",hdfs路径不存在[" + path + "]";
                flag = false;
            }
            boolean isDas = path.contains("/das/");
            if (isDas) {
                /*
                if (HadoopHdfsUtil.checkDasDataPathWithMsg(className, fileSystem, path) != 0) {
                    flag = false;
                }
                */
            }
            else {
                boolean isPmcs = path.contains("/user/ztbd/pmcs/pmcs_full/");
                /*
                if (isPmcs) {
                    if (HadoopHdfsUtil.checkPmcsDataPathWithMsg(className, fileSystem, path) != 0) {
                        flag = false;
                    }
                }
                else if (HadoopHdfsUtil.checkDataPathWithMsg(className, fileSystem, path) != 0) {
                    flag = false;
                }
                */
            }

            logger.info("--> hdfs path:" + path);
        }
        return flag;
    }

    public static int getHdfsFileSize(FileSystem fileSystem, String path)
    {
        FileStatus[] fs = HadoopHdfsUtil.getFilesStatus(fileSystem, path);
        if (fs == null) {
            return 0;
        }
        return fs.length;
    }

    public static SparkConf initSparkConf(String className, String[] args, int length)
    {
        return initSparkConf(className, args, length, null);
    }

    public static SparkConf initSparkConf(String className, String[] args, int length, String service)
    {
        Preconditions.checkArgument(args.length >= length, "runEnvFlag 没有设置。(运行环境标识：DEV代表本地开发环境、PRE代表测试环境、PRD代表生产环境)");
        PropertyUtil.setCommFlags(args[0]);
        hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");

        SparkConf sparkConf = new SparkConf().setAppName(className);
        if (PropertyUtil.RunEnvFlagEnum.DEV == PropertyUtil.getRunEnvFlag())
            sparkConf.setMaster("local[1]");
        else {
            sparkConf.setMaster("yarn");
        }
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "com.suning.search.dps.util.RryoRegistrator");
        sparkConf.set("spark.shuffle.consolidateFiles", "true");
        if (StringUtils.isEmpty(service))
            compareZkKey = "/zkState/DPS/compare/" + className;
        else {
            compareZkKey = "/zkState/DPS/compare/" + service;
        }
        isPrd = "prd".equalsIgnoreCase(args[0]);
        return sparkConf;
    }

    public static String getHdfsAllPath(FileSystem fileSystem, String serviceHdfsPath, int serviceType, int dayType)
    {
        String hdfsPath = "";
        String dayTimeStr = DateUtil.getTime(dayType);
        switch (serviceType) {
            case 0:
                hdfsPath = hdfsBaseUrl + serviceHdfsPath;
                if (!HadoopHdfsUtil.checkHdfsSuccess(fileSystem, hdfsPath))
                    hdfsPath = null; break;
            case 1:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + serviceHdfsPath, dayTimeStr);
                break;
            case 2:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + serviceHdfsPath, dayTimeStr);
                String lastTime = HadoopHdfsUtil.getLastSucTimePath(fileSystem, hdfsPath);
                if (!hdfsPath.endsWith("/"))
                    hdfsPath = hdfsPath + File.separator + lastTime;
                else {
                    hdfsPath = hdfsPath + lastTime;
                }
                break;
            case 3:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + serviceHdfsPath, dayTimeStr);
                String lastSucTime = HadoopHdfsUtil.getLastSucTimePath(fileSystem, hdfsPath);
                if (!hdfsPath.endsWith("/"))
                    hdfsPath = hdfsPath + File.separator + lastSucTime;
                else {
                    hdfsPath = hdfsPath + lastSucTime;
                }
                break;
            case 4:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + serviceHdfsPath, currentDateStr);
                if ((!checkHdfsPath(fileSystem, hdfsPath)) || (HadoopHdfsUtil.getSucTimePath(fileSystem, hdfsPath).size() == 0)) {
                    if (CommUtil.inTimeRange("11:00", "18:00")) {
                        if (hdfsPath.contains("/das/"))
                            MessageUtil.sendMsg("DasDataCheck", "IDE类=" + className + "，路径=" + hdfsPath + "没有数据或者没有成功标识,今天没有数据产生，注意检查相关采集项调度。");
                        else {
                            MessageUtil.sendMsg("DasDpsDataCheck", "IDE类=" + className + "，路径=" + hdfsPath + "没有数据或者没有成功标识今天没有数据产生，注意检查相关任务流调度。");
                        }
                    }
                    hdfsPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + serviceHdfsPath, yesdayDateStr);
                }
                String lastSucTime2 = HadoopHdfsUtil.getLastSucTimePath(fileSystem, hdfsPath);
                if (!hdfsPath.endsWith("/"))
                    hdfsPath = hdfsPath + File.separator + lastSucTime2;
                else {
                    hdfsPath = hdfsPath + lastSucTime2;
                }
                break;
        }

        if (checkHdfsPath(fileSystem, hdfsPath)) {
            return hdfsPath;
        }
        return hdfsPath + "[error]";
    }

    public static String getHdfsAllPath(FileSystem fileSystem, String serviceHdfsPath, int serviceType)
    {
        return getHdfsAllPath(fileSystem, serviceHdfsPath, serviceType, 0);
    }

    public static String getPmcsPath(String path)
    {
        String priceDateStr = "date=" + DateUtil.getCurrentDate("yyyy-MM-dd");
        String tmpPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + path, priceDateStr);
        String lastStr = HadoopHdfsUtil.getLastSucTime(fileSystem, tmpPath);
        PMCS_VERSION = lastStr;
        if (StringUtils.isEmpty(lastStr)) {
            lastStr = "null";
        }
        return tmpPath + lastStr;
    }

    public static String getContractPath(String path)
    {
        String priceDateStr = "date=" + DateUtil.getCurrentDate("yyyy-MM-dd");
        String tmpPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + path, priceDateStr);
        String lastStr = HadoopHdfsUtil.getLastSucTimePath(fileSystem, tmpPath);
        PMCS_VERSION = lastStr;
        if (StringUtils.isEmpty(lastStr)) {
            lastStr = "null";
        }
        return tmpPath + lastStr;
    }

    public static String getCurAndYesDayContractPath(String path)
    {
        String priceDateStr = "date=" + DateUtil.getCurrentDate("yyyy-MM-dd");
        String tmpPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + path, priceDateStr);
        String lastStr = HadoopHdfsUtil.getLastSucTimePath(fileSystem, tmpPath);
        PMCS_VERSION = lastStr;
        if (StringUtils.isNotBlank(lastStr)) {
            return tmpPath + lastStr;
        }
        return getContractPath(path, -1);
    }

    public static String getContractPath(String path, int day)
    {
        String priceDateStr = "date=" + DateUtil.getTime(day, "yyyy-MM-dd");
        String tmpPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + path, priceDateStr);
        String lastStr = HadoopHdfsUtil.getLastSucTimePath(fileSystem, tmpPath);
        PMCS_VERSION = lastStr;
        return tmpPath + lastStr + "/";
    }

    public static String getYesdayContractPath(String path)
    {
        String priceDateStr = "date=" + DateUtil.getTime(-1, "yyyy-MM-dd");
        String tmpPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + path, priceDateStr);
        String lastStr = HadoopHdfsUtil.getLastSucTimePath(fileSystem, tmpPath);
        return tmpPath + lastStr;
    }

    public static void parseOutputHdfsPath(FileSystem fileSystem, String outPutAllHdfs, String outPutIncHdfs, int serviceType)
    {
        switch (serviceType) {
            case 0:
                fullOutputPath = hdfsBaseUrl + outPutAllHdfs;
                if (StringUtils.isNotEmpty(outPutIncHdfs))
                    incOutputPath = hdfsBaseUrl + outPutIncHdfs; break;
            case 1:
                fullOutputPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + outPutAllHdfs, currentDateStr);
                if (StringUtils.isNotEmpty(outPutIncHdfs))
                    incOutputPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + outPutIncHdfs, currentDateStr); break;
            case 2:
                fullOutputPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + outPutAllHdfs, currentDateStr);
                if (!fullOutputPath.endsWith("/"))
                    fullOutputPath = fullOutputPath + File.separator + DateUtil.getTimeStr();
                else {
                    fullOutputPath += DateUtil.getTimeStr();
                }
                if (StringUtils.isNotEmpty(outPutIncHdfs)) {
                    incOutputPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + outPutIncHdfs, currentDateStr);
                    if (!incOutputPath.endsWith("/"))
                        incOutputPath = incOutputPath + File.separator + DateUtil.getTimeStr();
                    else
                        incOutputPath += DateUtil.getTimeStr();  } break;
        }

        delHdfsPath(fileSystem, fullOutputPath);
        if (StringUtils.isNotEmpty(outPutIncHdfs)) {
            delHdfsPath(fileSystem, incOutputPath);
            lastOutputPath = ZKClientUtil.getZkContentByPath(compareZkKey);
            logger.info("--> incOutputPath :" + incOutputPath);
        }
        if ((!"null".equals(lastOutputPath)) && (StringUtils.isNotEmpty(lastOutputPath)) && (!fullOutputPath.equals(lastOutputPath))) {
            flag = true;
        }
        logger.info("--> fullOutputPath :" + fullOutputPath);
    }

    public static String getOutputHdfsPath(FileSystem fileSystem, String outPutAllHdfs, int serviceType, int saveNum)
    {
        String outPut = getOutputHdfsPath(fileSystem, outPutAllHdfs, serviceType);
        if (saveNum != -1) {
            HadoopHdfsUtil.clearHdfs(hdfsBaseUrl + outPutAllHdfs, fileSystem, saveNum);
        }
        return outPut;
    }

    public static String getOutputHdfsPath(FileSystem fileSystem, String outPutAllHdfs, int serviceType)
    {
        String hdfsPath = "";
        switch (serviceType) {
            case 0:
                hdfsPath = hdfsBaseUrl + outPutAllHdfs;
                break;
            case 1:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + outPutAllHdfs, currentDateStr);
                break;
            case 2:
                String temp = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + outPutAllHdfs, currentDateStr);
                outputTimeStr = DateUtil.getTimeStr();
                if (!temp.endsWith("/"))
                    hdfsPath = temp + File.separator + outputTimeStr;
                else {
                    hdfsPath = temp + outputTimeStr;
                }
                break;
            case 5:
                String temp2 = hdfsBaseUrl + outPutAllHdfs;
                if (!temp2.endsWith("/"))
                    hdfsPath = temp2 + File.separator + DateUtil.getCurrentDate("yyyyMMddHHmmss");
                else {
                    hdfsPath = temp2 + DateUtil.getCurrentDate("yyyyMMddHHmmss");
                }
            case 6:
                String temp3 = hdfsBaseUrl + outPutAllHdfs;
                if (!temp3.endsWith("/"))
                    hdfsPath = temp3 + File.separator + version;
                else
                    hdfsPath = temp3 + version;
                break;
            case 3:
            case 4:
        }
        delHdfsPath(fileSystem, hdfsPath);
        return hdfsPath;
    }

    public static String getInputHdfsPath(FileSystem fileSystem, String outPutAllHdfs, int serviceType, int dayType)
    {
        String dayTimeStr = DateUtil.getTime(dayType);
        String hdfsPath = "";
        switch (serviceType) {
            case 0:
                hdfsPath = hdfsBaseUrl + outPutAllHdfs;
                break;
            case 1:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(hdfsBaseUrl + outPutAllHdfs, dayTimeStr);
                break;
            case 2:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(new StringBuilder().append(hdfsBaseUrl).append(outPutAllHdfs).toString(), dayTimeStr) + File.separator + DateUtil.getTimeStr();
                break;
        }

        return hdfsPath;
    }

    public static boolean checkHdfsPath(FileSystem fileSystem, String hdfsPath) {
        boolean flag = false;
        try {
            flag = fileSystem.exists(new Path(hdfsPath));
        } catch (Exception e) {
            logger.error("检查路径:" + e.getMessage(), e);
        }
        return flag;
    }

    public static void putStrting2Json(JSONObject resultJson, String key, String value) {
        if ((StringUtils.isNotEmpty(value)) && (!"null".equals(value)))
            resultJson.put(key, value);
    }

    public static boolean checkHdfsPath(FileSystem fileSystem, String[] hdfsPaths)
    {
        boolean flag = true;
        for (String hdfsPath : hdfsPaths) {
            if (!checkHdfsPath(fileSystem, hdfsPath)) {
                return false;
            }
        }
        return flag;
    }

    public static void delHdfsPath(FileSystem fileSystem, String fullOutputPath) {
        try {
            if (fileSystem.exists(new Path(fullOutputPath))) {
                logger.info("先删除，已经存在的数据:" + fullOutputPath);
                fileSystem.delete(new Path(fullOutputPath), true);
            }
        } catch (IllegalArgumentException e) {
            logger.info("先删除，已经存在的数据:" + fullOutputPath);
        } catch (IOException e) {
            logger.info("先删除，已经存在的数据:" + fullOutputPath);
        }
    }

    public static void checkImsZk(String[] args, String systemCode)
    {
        String dpsPath = "/zkState/DPS/" + systemCode + "/check/full";
        String dpsStatus = ZKClientUtil.getZkContentByPath(dpsPath);
        JSONObject obj = null;
        if ((dpsStatus == null) || (dpsStatus.trim().length() == 0)) {
            obj = new JSONObject();
            obj.put("version", DateUtil.getTodayTime() + "01");
        } else {
            obj = JSONObject.fromObject(dpsStatus);
            obj.put("version", String.valueOf(Integer.parseInt(obj.getString("version")) + 1));
        }
        String count = CommUtil.getNodeValue(obj, "allCount");

        obj.put("allCount", count);
        obj.put("startStrTime", allStartDateStr);
        obj.put("startLongTime", Long.valueOf(new Date().getTime()));
        obj.put("state", "0");
        ZKClientUtil.updateNode(dpsPath, obj.toString());
        ZKClientUtil.closeZkCient();
    }

    public static void checkImsTimeZk(String[] args, String systemCode)
    {
        version = DateUtil.getCurrentDate("yyyyMMddHHmmss");

        String dpsPath = "/zkState/DPS/" + systemCode + "/check/full";
        String dpsStatus = ZKClientUtil.getZkContentByPath(dpsPath);
        JSONObject obj = null;
        if ((dpsStatus == null) || (dpsStatus.trim().length() == 0))
            obj = new JSONObject();
        else {
            obj = JSONObject.fromObject(dpsStatus);
        }
        String count = CommUtil.getNodeValue(obj, "allCount");
        obj.put("version", version);
        obj.put("allCount", count);
        obj.put("startStrTime", allStartDateStr);
        obj.put("allStartVersionStr", allStartVersionStr);
        obj.put("startLongTime", Long.valueOf(new Date().getTime()));
        obj.put("state", "0");
        ZKClientUtil.updateNode(dpsPath, obj.toString());
        ZKClientUtil.closeZkCient();
    }

    public static void checkCouponImsTimeZk(String systemCode)
    {
        version = DateUtil.getCurrentDate("yyyyMMddHHmmss");
        String dpsPath = "/zkState/DPS/" + systemCode + "/check/full";
        String dpsStatus = ZKClientUtil.getZkContentByPath(dpsPath);
        JSONObject obj = null;
        if ((dpsStatus == null) || (dpsStatus.trim().length() == 0))
            obj = new JSONObject();
        else {
            obj = JSONObject.fromObject(dpsStatus);
        }
        String count = CommUtil.getNodeValue(obj, "allCount");
        String batchzNum = CommUtil.getNodeValue(obj, "batchzNum");
        if (StringUtils.isEmpty(batchzNum)) {
            batchzNum = "0";
        }
        obj.put("version", version);
        obj.put("allCount", count);
        obj.put("startStrTime", allStartDateStr);
        obj.put("allStartVersionStr", allStartVersionStr);
        obj.put("startLongTime", Long.valueOf(new Date().getTime()));
        obj.put("state", "1");
        int batch = Integer.parseInt(batchzNum) + 1;
        if (batch > 1000000) {
            batch = 1;
        }
        obj.put("batchzNum", String.valueOf(batch));
        ZKClientUtil.updateNode(dpsPath, obj.toString());
        ZKClientUtil.closeZkCient();
    }

    public static String getAllStartTimeByZk(String systemCode)
    {
        String dpsPath = "/zkState/DPS/" + systemCode + "/check/full";
        String dpsStatus = ZKClientUtil.getZkContentByPath(dpsPath);
        JSONObject obj = JSONObject.fromObject(dpsStatus);
        String startStrTime = CommUtil.getNodeValue(obj, "startStrTime");
        return startStrTime;
    }

    public static String getVersionByZk(String systemCode)
    {
        String dpsPath = "/zkState/DPS/" + systemCode + "/check/full";
        String dpsStatus = ZKClientUtil.getZkContentByPath(dpsPath);
        JSONObject obj = JSONObject.fromObject(dpsStatus);
        String startStrTime = CommUtil.getNodeValue(obj, "version");
        return startStrTime;
    }

    public static String getAllVersionByZk(String systemCode)
    {
        String dpsPath = "/zkState/DPS/" + systemCode + "/check/full";
        String dpsStatus = ZKClientUtil.getZkContentByPath(dpsPath);
        JSONObject obj = JSONObject.fromObject(dpsStatus);
        String startStrTime = CommUtil.getNodeValue(obj, "allStartVersionStr");
        return startStrTime;
    }

    public static void addCurCount2Zk(long count, String systemCode)
    {
        String dpsPath = "/zkState/DPS/" + systemCode + "/check/full";
        String dpsStatus = ZKClientUtil.getZkContentByPath(dpsPath);
        try {
            if (StringUtils.isNotBlank(dpsStatus)) {
                JSONObject obj = JSONObject.fromObject(dpsStatus);
                obj.put("curAllCount", String.valueOf(count));
                ZKClientUtil.updateNode(dpsPath, obj.toString());
            }
        } catch (Exception e) {
            logger.error("设置本次全量数据量报错:", e);
        }
        ZKClientUtil.closeZkCient();
    }

    public static boolean checkBrand(String str, String zfBrandId)
    {
        boolean flag = false;
        try {
            if ((StringUtils.isNotBlank(str)) && (!"null".equals(str)) && (!"{}".equals(str))) {
                JSONArray arrays = JSONArray.fromObject(str);
                for (Iterator tor = arrays.iterator(); tor.hasNext(); ) {
                    String item = (String)tor.next();
                    if (item.startsWith(zfBrandId))
                        flag = true;
                }
            }
        }
        catch (Exception e)
        {
            Iterator tor;
            logger.error("解析品牌报错==>" + str);
        }
        return flag;
    }

    public static String parseMultiStr(String str)
    {
        if ((StringUtils.isNotBlank(str)) && (!"null".equals(str)) && (!"{}".equals(str))) {
            String[] array = str.split(",");
            Map jsonMap = new HashMap();
            for (String s : array) {
                if ((StringUtils.isNotBlank(s)) && (!"null".equals(s)) && (!"{}".equals(s))) {
                    CommUtil.addItem2MapSet(jsonMap, "key", s);
                }
            }
            return CommUtil.getJsonString(jsonMap);
        }
        return "";
    }

    public static String parseMultiStr(String str, String split)
    {
        if ((StringUtils.isNotBlank(str)) && (!"null".equals(str)) && (!"{}".equals(str))) {
            String[] array = str.split(split);
            Map jsonMap = new HashMap();
            for (String s : array) {
                if ((StringUtils.isNotBlank(s)) && (!"null".equals(s)) && (!"{}".equals(s))) {
                    CommUtil.addItem2MapSet(jsonMap, "key", s);
                }
            }
            return CommUtil.getJsonString(jsonMap);
        }
        return "";
    }

    public static String parseMultiGroupStr(String str, String groupId)
    {
        if (!CommUtil.isNotBlankAndNull(groupId)) {
            return parseMultiStr(str);
        }
        if ((StringUtils.isNotBlank(str)) && (!"null".equals(str)) && (!"{}".equals(str))) {
            String[] array = str.split(",");
            String[] groupIds = groupId.split(",");
            Map jsonMap = new HashMap();
            for (String s : array) {
                if ((StringUtils.isNotBlank(s)) && (!"null".equals(s)) && (!"{}".equals(s)) && (checkGroup(groupIds, s))) {
                    CommUtil.addItem2MapSet(jsonMap, "key", s);
                }
            }

            return CommUtil.getJsonString(jsonMap);
        }

        return "";
    }

    private static boolean checkGroup(String[] groupIds, String s) {
        boolean isFlag = true;
        for (String group : groupIds) {
            if ((s + ":").endsWith(":" + group + ":")) {
                isFlag = false;
            }
        }
        return isFlag;
    }

    public static String parseMultiSortList(String str)
    {
        if ((StringUtils.isNotBlank(str)) && (!"null".equals(str))) {
            String[] array = str.split(",");
            List list = Arrays.asList(array);
            Collections.sort(list);
            Map jsonMap = new HashMap();
            jsonMap.put("key", list);

            return CommUtil.getJsonString(jsonMap);
        }
        return "";
    }

    public static boolean compareData(JavaRDD<String> tadayRdd, JavaRDD<String> yesdayRdd, String incPath, String splitStr, String primaryKey, String delStr, int delFlagIndex, int numPartitions, int updateTimeIndex, boolean appendFlag)
    {
        boolean flag = false;
        JavaPairRDD tadayRddPairMap = tadayRdd.mapPartitionsToPair(new MyPairFlatMapFunction(splitStr, primaryKey));
        JavaPairRDD yesdayRddPairMap = yesdayRdd.mapPartitionsToPair(new MyPairFlatMapFunction(splitStr, primaryKey));
        JavaPairRDD javaPairRDDJoin = tadayRddPairMap.cogroup(yesdayRddPairMap);
        JavaRDD javaRDD = javaPairRDDJoin.mapPartitions(new MyCompareFunction(delStr, delFlagIndex, splitStr, updateTimeIndex, appendFlag));
        if ((javaRDD != null) && (!javaRDD.isEmpty())) {
            javaRDD.repartition(numPartitions).saveAsTextFile(incPath);
            flag = true;
        }
        return flag;
    }

    public static boolean compareData2(String curPath, String lastPath, String incPath, String incListPath, String splitStr, String primaryKey, String delStr, int delFlagIndex, int numPartitions, String updateTimeIndex, Map<Integer, String> exceptMap)
    {
        boolean flag = false;

        if ((StringUtils.isNotBlank(curPath)) && (StringUtils.isNotBlank(lastPath)) && (!curPath.equals(lastPath))) {
            JavaRDD tadayRdd = javaSparkContext.textFile(curPath);
            JavaRDD yesdayRdd = javaSparkContext.textFile(lastPath);
            JavaPairRDD tadayRddPairMap = tadayRdd.mapToPair(new MyParsePairFunction(splitStr, primaryKey)).filter(new EmptyFilterFunction());
            JavaPairRDD yesdayRddPairMap = yesdayRdd.mapToPair(new MyParsePairFunction(splitStr, primaryKey)).filter(new EmptyFilterFunction());
            JavaPairRDD javaPairRDDJoin = tadayRddPairMap.cogroup(yesdayRddPairMap);
            JavaRDD javaRDD2 = javaPairRDDJoin.mapPartitions(new MyComparePairFunction(delStr, delFlagIndex, splitStr, updateTimeIndex, exceptMap)).filter(new EmptyStringFilterFunction());

            if ((javaRDD2 != null) && (!javaRDD2.isEmpty())) {
                javaRDD2.repartition(numPartitions).saveAsTextFile(incPath);
                if (StringUtils.isNotBlank(incListPath)) {
                    javaRDD2.repartition(numPartitions).saveAsTextFile(incListPath);
                }
                flag = true;
            }
        }
        return flag;
    }

    public static Map<String, String> getTicketShardMap(String configPath)
    {
        Map vmap = new HashMap();
        String shardValue = ZKClientUtil.getZkContentByPath(configPath);
        logger.info("shardValue==>" + shardValue);
        ByteArrayInputStream is2 = new ByteArrayInputStream(shardValue.getBytes());
        Properties p2 = new Properties();
        try {
            p2.load(is2);
            String allShard = p2.getProperty("ALL_FRONT_SHARD");
            String[] array = allShard.split(",");
            for (String ca : array)
                if (!StringUtils.isEmpty(ca))
                {
                    String v = p2.getProperty(ca);
                    JSONObject json = JSONObject.fromObject(v);
                    String _shardNums = CommUtil.getJsonVal(json, "shardNums");
                    String[] shards = _shardNums.split(",");
                    for (String sh : shards)
                        if (StringUtils.isNotBlank(sh))
                            vmap.put(sh, ca);
                }
        }
        catch (IOException e)
        {
            logger.error("读取配置文件报错", e);
        }
        return vmap;
    }

    public static String parseMultiStr(String str, boolean isLLPGFlag, Set<String> gwjtIds)
    {
        try
        {
            if ((StringUtils.isNotBlank(str)) && (!"null".equals(str)) && (!"{}".equals(str)) && (isLLPGFlag) && (!gwjtIds.isEmpty())) {
                String[] array = str.split(",");
                Map jsonMap = new HashMap();
                String[] arr$ = array; int len$ = arr$.length; int i$ = 0; if (i$ < len$) { String s = arr$[i$];
                    if ((StringUtils.isNotBlank(s)) && (!"null".equals(s)) && (!"{}".equals(s))) {
                        String[] acIds = s.split("@@");
                        if (!gwjtIds.contains(acIds[0])) {
                            CommUtil.addItem2MapSet(jsonMap, "key", s);
                        }
                    }
                    return CommUtil.getJsonString(jsonMap); }
            }
        }
        catch (Exception e) {
            logger.error("购物津贴数据处理报错" + str);
        }
        return parseMultiStr(str);
    }

    public static String parseActivity(Map<String, Object> activityMap, boolean isLLPGFlag, Set<String> gwjtIds)
    {
        try
        {
            if ((isLLPGFlag) && (!gwjtIds.isEmpty())) {
                Map _activityMap = new TreeMap();
                for (String key : activityMap.keySet()) {
                    String[] ks = key.split("_");
                    String acId = ks[0];
                    if (!gwjtIds.contains(acId)) {
                        _activityMap.put(key, activityMap.get(key));
                    }
                }
                return JSONObject.fromObject(_activityMap).toString();
            }
        } catch (Exception e) {
            logger.error("购物津贴数据处理报错" + e.getMessage());
        }
        return JSONObject.fromObject(activityMap).toString();
    }

    public static void excute()
            throws Exception
    {
    }
}