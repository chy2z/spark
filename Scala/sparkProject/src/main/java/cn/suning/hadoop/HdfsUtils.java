package cn.suning.hadoop;

import java.io.File;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HdfsUtils
{
    private static Logger logger = Logger.getLogger(HdfsUtils.class);

    public static String currentDateStr = DateUtil.getTime(0);

    public static String yesdayDateStr = DateUtil.getTime(-1);

    public static String getAllPath(FileSystem fileSystem, String serviceHdfsPath, int serviceType)
    {
        return getHdfsAllPath(fileSystem, serviceHdfsPath, serviceType, 0);
    }

    public static String getHdfsAllPath(FileSystem fileSystem, String serviceHdfsPath, int serviceType, int dayType)
    {
        String hdfsPath = "";
        String dayTimeStr = DateUtil.getTime(dayType);
        switch (serviceType) {
            case 0:
                hdfsPath = serviceHdfsPath;
                if (!HadoopHdfsUtil.checkHdfsSuccess(fileSystem, hdfsPath))
                    hdfsPath = null; break;
            case 1:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(serviceHdfsPath, dayTimeStr);
                break;
            case 2:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(serviceHdfsPath, dayTimeStr);
                String lastTime = HadoopHdfsUtil.getLastSucTimePath(fileSystem, hdfsPath);
                if (!hdfsPath.endsWith("/"))
                    hdfsPath = hdfsPath + File.separator + lastTime;
                else {
                    hdfsPath = hdfsPath + lastTime;
                }
                break;
            case 3:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(serviceHdfsPath, dayTimeStr);
                String lastSucTime = HadoopHdfsUtil.getLastSucTimePath(fileSystem, hdfsPath);
                if (!hdfsPath.endsWith("/"))
                    hdfsPath = hdfsPath + File.separator + lastSucTime;
                else {
                    hdfsPath = hdfsPath + lastSucTime;
                }
                break;
            case 4:
                hdfsPath = HadoopHdfsUtil.inputDataPathChange(serviceHdfsPath, currentDateStr);
                logger.info("hdfsPath:" + hdfsPath);
                if ((!checkHdfsPath(fileSystem, hdfsPath)) || (HadoopHdfsUtil.getSucTimePath(fileSystem, hdfsPath).size() == 0))
                {
                    hdfsPath = HadoopHdfsUtil.inputDataPathChange(serviceHdfsPath, yesdayDateStr);
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
    public static boolean checkHdfsPath(FileSystem fileSystem, String hdfsPath) {
        boolean flag = false;
        try {
            flag = fileSystem.exists(new Path(hdfsPath));
        } catch (Exception e) {
            logger.error("检查路径:" + e.getMessage(), e);
        }
        return flag;
    }
}