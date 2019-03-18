package cn.suning.hadoop;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class PropertyUtil
{
    private static Logger logger = Logger.getLogger(PropertyUtil.class);

    private Properties properties = new Properties();
    private static RunEnvFlagEnum runEnvFlag;
    private static DataTypeEnum dataType;

    private PropertyUtil()
    {
        try
        {
            InputStream in = PropertyUtil.class.getClassLoader().getResourceAsStream("conf.properties");
            this.properties.load(in);
        } catch (Exception e) {
            logger.error("加载conf.properties出错:", e);
        }
    }

    private static PropertyUtil getInstance()
    {
        return SingletonHolder.instance;
    }

    public static String getProperty(String key)
    {
        return getInstance().properties.getProperty(key + "_" + runEnvFlag.name().toLowerCase());
    }

    public static String getPropertyRaw(String key)
    {
        return getInstance().properties.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue)
    {
        String result = "";
        String val = getInstance().properties.getProperty(key + "_" + runEnvFlag.name().toLowerCase());
        if (StringUtils.isBlank(val))
            result = defaultValue;
        else {
            result = val;
        }
        return result;
    }

    public static RunEnvFlagEnum getRunEnvFlag()
    {
        return runEnvFlag;
    }

    public static DataTypeEnum getDataType()
    {
        return dataType;
    }

    public static void setCommFlags(String commFlagStr)
    {
        dataType = DataTypeEnum.NORMAL;

        String[] commFlags = commFlagStr.split(",");

        String envFlag = commFlags[0].trim().toLowerCase();
        if ("dev".equals(envFlag))
            runEnvFlag = RunEnvFlagEnum.DEV;
        else if ("pre".equals(envFlag))
            runEnvFlag = RunEnvFlagEnum.PRE;
        else if ("prd".equals(envFlag))
            runEnvFlag = RunEnvFlagEnum.PRD;
        else if ("xgpre".equals(envFlag))
            runEnvFlag = RunEnvFlagEnum.XGPRE;
        else if ("sit".equals(envFlag)) {
            runEnvFlag = RunEnvFlagEnum.PRE;
        }
        if (2 == commFlags.length) {
            String tmp = commFlags[1].trim().toLowerCase();
            if ("test".equals(tmp))
            {
                dataType = DataTypeEnum.TEST;
            }
        }

        logger.info("runEnvFlag:" + getRunEnvFlag());
        logger.info("dataType:" + getDataType());
        logger.info("hdfsBaseUrl:" + getProperty("hdfsBaseUrl"));
        logger.info("phoenixJdbcUrl:" + getProperty("phoenixJdbcUrl"));
        logger.info("zkIps:" + getProperty("zkIps"));
    }

    public static void main(String[] args) throws Exception {
        setCommFlags("dev,");
        System.out.println(getProperty("hdfsBaseUrl"));
        if (RunEnvFlagEnum.DEV == getRunEnvFlag())
            System.out.println("=====");
    }

    private static class SingletonHolder {
        private static PropertyUtil instance = new PropertyUtil();
    }

    public static enum DataTypeEnum
    {
        NORMAL,
        TEST;
    }

    public static enum RunEnvFlagEnum
    {
        DEV,
        SIT,
        PRE,
        PRD,
        XGPRE,
        PRD_TEST;
    }
}