package cn.suning.hadoop;

import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DateUtil.class);
    public static final String TIME_PATTERN_YMD = "yyyy-MM-dd";
    public static final String TIME_PATTERN_YYYYMMDD = "yyyy-MM-dd";
    public static final String TIME_PATTERN_YMDHMS = "yyyy-MM-dd HH:mm:ss";
    public static final String TIME_PATTERN_YMDHMSS = "yyyy-MM-dd HH:mm:ssS";
    public static final String TIME_PATTERN_YMDHMS_P = "yyyy/MM/dd HH:mm:ss";
    public static final String TIME_PATTERNYMDHMS = "yyyyMMddHHmmss";
    public static final String TIME_PATTERNYMD = "yyyyMMdd";
    public static final String TIME_PATTERNYMD_TICKET = "yyMMddHHmm";
    public static final String TIME_YY = "HH";
    public static final String TIME_HHMMSS = "HHmmssSSS";
    public static final String DEFAULT_TIME = "1970-08-08 16:16:16";

    public static int getCurMonth()
    {
        SimpleDateFormat formatter = new SimpleDateFormat("MM");
        String dateString = formatter.format(new Date());
        if (dateString.startsWith("0")) {
            dateString = dateString.substring(1, 2);
        }
        return Integer.parseInt(dateString);
    }

    public static void main(String[] args) {
        System.out.println(curTimeAdd(10));

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
        String dateString = formatter.format(new Date());
        System.out.println(Integer.parseInt(dateString) + 1 + "-12-30 00:00:00");
        Long endTime = Long.valueOf(parseString2Date(Integer.parseInt(dateString) + 1 + "-12-30 00:00:00", "yyyy-MM-dd HH:mm:ss").getTime());

        System.out.println(endTime);
    }

    public static String getManTime(int minute)
    {
        Date date = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(12, minute);
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());
    }

    public static String getDateTime(Date date)
    {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String dateString = formatter.format(date);
        return dateString;
    }

    public static String getTodayTime()
    {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String dateString = formatter.format(new Date());
        return dateString;
    }

    public static String getTimeStr()
    {
        SimpleDateFormat formatter = new SimpleDateFormat("HHmmssSSS");
        return formatter.format(new Date());
    }

    public static String getCurrentDate(String pattern)
    {
        String dateString = getDateFormartString(pattern, new Date());
        return dateString;
    }

    public static String getReverseVersion()
    {
        String timeStr = String.valueOf(new Date().getTime());
        return reverse(timeStr);
    }

    public static String reverse(String str)
    {
        return new StringBuffer(str).reverse().toString();
    }

    public static String getDateFormartString(String pattern, Date date)
    {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);
        String dateString = formatter.format(date);
        return dateString;
    }

    public static String curTimeAdd(int hour)
    {
        Date date = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(12, hour);
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());
    }

    public static String parseDateTime(Date date, String parttern)
    {
        SimpleDateFormat formatter = new SimpleDateFormat(parttern);
        String dateString = formatter.format(date);
        return dateString;
    }

    public static String getTime(int ftpFiledateType) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(5, ftpFiledateType);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        return formatter.format(calendar.getTime());
    }

    public static String getTime(int ftpFiledateType, String parrent) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(5, ftpFiledateType);
        SimpleDateFormat formatter = new SimpleDateFormat(parrent);
        return formatter.format(calendar.getTime());
    }

    public static String getDateStr(String dateStr, int dayNum) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
            Date date = formatter.parse(dateStr);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(5, dayNum);
            return formatter.format(calendar.getTime()); } catch (Exception e) {
        }
        return "";
    }

    public static Date parseString2Date(String str, String pattern1, String pattern2)
    {
        SimpleDateFormat format = new SimpleDateFormat(pattern1);
        Date date = null;
        try {
            date = format.parse(str);
        } catch (Exception e) {
            format = new SimpleDateFormat(pattern2);
            date = null;
            try {
                date = format.parse(str);
            } catch (Exception e1) {
                LOG.error("时间转换报错." + e1.getMessage());
                date = new Date();
            }
        }
        return date;
    }

    public static Date parseString2Date(String str, String pattern1) {
        SimpleDateFormat format = new SimpleDateFormat(pattern1);
        Date date = null;
        try
        {
            date = format.parse(str);
        } catch (Exception e) {
            LOG.error("时间转换报错." + e.getMessage());
        }
        return date;
    }
    public static Date parseString2Date(String str) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse(str);
        } catch (Exception e) {
            LOG.error("时间转换报错." + e.getMessage());
        }
        return date;
    }

    public static String getNotRuleTimeStr(String v)
    {
        if (v.length() > 20) {
            v = v.substring(0, 19);
            try {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                formatter.format(v);
            } catch (Exception e) {
                v = "2010-01-01 08:00:00";
            }
        }
        return v;
    }

    public static boolean compareTimeStr(String t1, String t2)
    {
        long beginTime = parseString2Date(t1, "yyyy-MM-dd HH:mm:ss").getTime();
        long endTime = parseString2Date(t2, "yyyy-MM-dd HH:mm:ss").getTime();
        Date now = new Date();
        long nowTime = now.getTime();
        if ((nowTime >= beginTime) && (nowTime <= endTime)) {
            return true;
        }
        return false;
    }

    public static boolean compareTime(String t1, String t2)
    {
        long beginTime = parseString2Date(t1, "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss").getTime();
        long endTime = parseString2Date(t2, "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss").getTime();
        Date now = new Date();
        long nowTime = now.getTime();
        if ((nowTime >= beginTime) && (nowTime <= endTime)) {
            return true;
        }
        return false;
    }

    public static boolean compareBidPartyTime(String t1, String t2)
    {
        long beginTime = parseString2Date(t1, "yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss").getTime();
        long endTime = parseString2Date(t2, "yyyy/MM/dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss").getTime();
        Date now = new Date();
        long nowTime = now.getTime();
        if ((nowTime >= beginTime) && (nowTime <= endTime)) {
            return true;
        }
        return false;
    }

    public static String parseTime(String str)
    {
        if ((StringUtils.isEmpty(str)) || ("null".equals(str)) || ("NULL".equals(str))) {
            return "1970-08-08 16:16:16";
        }
        if (str.length() <= 19) {
            return str;
        }
        return str.substring(0, 19);
    }

    public static String getFileTimeName(int ftpFiledateType, String patten)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.add(5, ftpFiledateType);
        SimpleDateFormat formatter = new SimpleDateFormat(patten);
        return formatter.format(calendar.getTime());
    }

    public static boolean inTimeRange(String startTime, String endTime)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
        try {
            Date nt = sdf.parse(sdf.format(new Date()));
            Date st = sdf.parse(startTime);
            Date et = sdf.parse(endTime);
            return (nt.after(st)) && (nt.before(et));
        } catch (ParseException e) {
            LOG.error("时间转换报错", e);
        }
        return false;
    }

    public static Date getDate(String timeStr)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return sdf.parse(timeStr);
        }
        catch (ParseException e) {
            LOG.error("时间转换报错", e);
        }return null;
    }
}