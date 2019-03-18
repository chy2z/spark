package cn.suning.spark.demo1.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang.StringUtils;


public class DpsReVersionUtil
{
    public static String getNowVersion(String sysType)
    {
        return ZKClientUtil.getZkContentByPath(new StringBuilder().append("/zkState/DPS/bigData/innerUse/fullVersion/dpsNowVersion_").append(sysType).toString());
    }

    public static String getNextVersion(String sysType)
    {
        String nextVersion = "";
        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String today = sdf.format(now);
        String dpsNowVersion = ZKClientUtil.getZkContentByPath(new StringBuilder().append("/zkState/DPS/bigData/innerUse/fullVersion/dpsNowVersion_").append(sysType).toString());
        if (StringUtils.isEmpty(dpsNowVersion)) {
            nextVersion = new StringBuilder().append(today).append("01").toString();
        }
        else if (Long.valueOf(new StringBuilder().append(today).append("01").toString()).longValue() > Long.valueOf(dpsNowVersion).longValue()) {
            nextVersion = new StringBuilder().append(today).append("01").toString();
        } else {
            String dateStr = dpsNowVersion.substring(0, 8);
            String serialNumStr = dpsNowVersion.substring(8, dpsNowVersion.length());
            int serialNum = Integer.valueOf(serialNumStr).intValue() + 1;
            nextVersion = new StringBuilder().append(dateStr).append(serialNum > 9 ? Integer.valueOf(serialNum) : new StringBuilder().append("0").append(serialNum).toString()).toString();
        }

        ZKClientUtil.updateNode(new StringBuilder().append("/zkState/DPS/bigData/innerUse/fullVersion/dpsNowVersion_").append(sysType).toString(), nextVersion);
        return nextVersion;
    }

    public static void main(String[] args) {
        System.out.println(getNextVersion("localLifeGoods"));
    }
}