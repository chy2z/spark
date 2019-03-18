package cn.suning.spark.demo1.util;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import cn.suning.hadoop.DateUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;


public class CommUtil
        implements Serializable
{
    private static final long serialVersionUID = -8683435646217756572L;
    private static Logger logger = Logger.getLogger(CommUtil.class);

    public static final char[] DB2_HEX_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    public static boolean inTimeRange(String startTime, String endTime)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
        try {
            Date nt = sdf.parse(sdf.format(new Date()));
            Date st = sdf.parse(startTime);
            Date et = sdf.parse(endTime);
            return (nt.after(st)) && (nt.before(et));
        } catch (ParseException e) {
            logger.error("时间转换错误", e);
        }
        return false;
    }

    public static String getStrTrim(String str)
    {
        if ((StringUtils.isEmpty(str)) || ("null".equals(str)) || ("NULL".equals(str))) {
            return "";
        }
        return str;
    }

    public static String parseHexStr(String str)
    {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return str;
        }
        byte[] binData;
        try
        {
            binData = str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        char[] chars = new char[binData.length * 2];
        for (int i = 0; i < binData.length; i++) {
            int n = binData[i];
            if (n < 0) {
                n += 256;
            }
            chars[(i * 2)] = DB2_HEX_CHARS[(n / 16)];
            chars[(i * 2 + 1)] = DB2_HEX_CHARS[(n % 16)];
        }
        return new String(chars);
    }

    public static String stringToHex(String intString)
    {
        try
        {
            byte[] bytes = intString.getBytes("utf-8");
            StringBuilder hexString = new StringBuilder(2 * bytes.length);
            for (int i = 0; i < bytes.length; i++) {
                String hexStr = String.format("%2X", new Object[] { Byte.valueOf(bytes[i]) });
                hexString.append(hexStr);
            }
            return hexString.toString(); } catch (UnsupportedEncodingException e) {
        }
        return "";
    }

    public static String replaceStr(String primStr)
    {
        if (StringUtils.isBlank(primStr)) {
            return "";
        }
        Pattern p = Pattern.compile("\t");
        Matcher m = p.matcher(primStr);
        return m.replaceAll(" ").trim();
    }

    public static String replaceStr2(String primStr)
    {
        if (StringUtils.isBlank(primStr)) {
            return "";
        }
        Pattern p = Pattern.compile("\t|\r|\n|\r\n");
        Matcher m = p.matcher(primStr);
        return m.replaceAll(" ").trim();
    }

    public static String replaceRuleStr(String primStr)
    {
        if (StringUtils.isBlank(primStr)) {
            return "";
        }
        Pattern p = Pattern.compile("\\t\\r\\n");
        Matcher m = p.matcher(primStr);
        return m.replaceAll(" ").trim();
    }

    public static String[] getStr2Array(String str)
    {
        if (StringUtils.isEmpty(getStrTrim(str))) {
            return new String[0];
        }
        return str.split(",");
    }

    public static void main(String[] args) {
        try {
            String sdfsd = "豆豆鞋男士休闲鞋一脚蹬防水板鞋pu潮鞋男鞋低帮懒人鞋平底男透气\n 黑色 42码";
            System.out.println(replaceStr2(sdfsd));
            boolean f = checkAttrLine("10400063309,0070183667,H4sIAAAAAAAAAKtWMjQwMLC0tNRRslIy1DHUMTfTMzDQ0dEx0jHQUaoFAO86jcsfAAAA,,,a", "10400063309,0070183666,H4sIAAAAAAAAAKtWMjQwMLC0tNRRslIy1DHUMTfTMzDQ0dEx0jHQUaoFAO86jcsfAAAA,,,a");
            System.out.println(f);
            System.out.println(new StringBuilder().append("  =").append(gzipStr("PC")).toString());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String gzipStr(String primStr)
            throws Exception
    {
        if ((null == primStr) || (0 == primStr.length())) {
            return "";
        }
        if ("null".equals(primStr)) {
            return "";
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(primStr.getBytes());
        } catch (Exception e) {
            throw e;
        } finally {
            if (gzip != null)
                try {
                    gzip.close();
                    gzip = null;
                }
                catch (Exception e) {
                }
        }
        String result = new BASE64Encoder().encode(out.toByteArray());
        return result.replace("\r\n", "").replace("\n", "");
    }

    public static String unGzipString(String compressedStr)
    {
        if (StringUtils.isBlank(compressedStr)) {
            return "";
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = null;
        GZIPInputStream ginzip = null;
        byte[] compressed = null;
        String decompressed = null;
        try {
            compressed = new BASE64Decoder().decodeBuffer(compressedStr);
            in = new ByteArrayInputStream(compressed);
            ginzip = new GZIPInputStream(in);
            byte[] buffer = new byte[1024];
            int offset = -1;
            while ((offset = ginzip.read(buffer)) != -1) {
                out.write(buffer, 0, offset);
            }
            out.flush();
            byte[] datas = out.toByteArray();
            decompressed = new String(datas, Charset.forName("utf-8"));
            datas = null;
        } catch (IOException e) {
            logger.error(new StringBuilder().append("GZIP解压：").append(compressedStr).append(" 时出错").toString(), e);
        } finally {
            if (ginzip != null) {
                try {
                    ginzip.close();
                } catch (IOException e) {
                } finally {
                    ginzip = null;
                }
            }
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                } finally {
                    in = null;
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                } finally {
                    out = null;
                }
            }
        }
        if (decompressed != null) {
            decompressed = decompressed.trim();
        }
        return decompressed;
    }

    public static String getShard(String v)
    {
        try
        {
            int shard = Integer.parseInt(v);
            return String.valueOf(shard + 10); } catch (Exception e) {
        }
        return "10";
    }

    public static String removeLastStr(String str)
    {
        if (StringUtils.isNotEmpty(str)) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    public static String removeLastStr(String str, String split)
    {
        if ((StringUtils.isNotBlank(str)) && (StringUtils.isNotBlank(split))) {
            String end = str.substring(str.length() - 1, str.length());
            if (end.equals(split)) {
                str = str.substring(0, str.length() - 1);
            }
        }
        return str;
    }

    public static List<String> sortSet(Set<String> set)
    {
        List list = new ArrayList();
        if ((set == null) || (set.isEmpty())) {
            return list;
        }
        list.addAll(set);
        Collections.sort(list, new Comparator<String>()
        {
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        return list;
    }

    public static String getCityStr(Set<String> set) {
        List<String> list = sortSet(set);
        String str = "";
        for (String city : list) {
            str = new StringBuilder().append(city).append(",").append(str).toString();
        }
        if (StringUtils.isNotBlank(str)) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    public static List<String> sortSet(String[] set)
    {
        List list = new ArrayList();
        if ((set == null) || (set.length == 0)) {
            return list;
        }
        for (String s : set) {
            list.add(s);
        }
        Collections.sort(list, new Comparator<String>()
        {
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        return list;
    }

    public static String getUpdateTime(boolean isFull, String v)
    {
        if (isFull) {
            return v;
        }
        String time = DateUtil.parseDateTime(new Date(), "yyyy-MM-dd HH:mm:ss");
        return time;
    }

    public static String setProductDefaultValue(String f, String v, String QUERY_FIELD_DOUBLE, String QUERY_FIELD_TIMESTAMP)
    {
        if ((StringUtils.isNotEmpty(v)) && (!v.equals("null"))) {
            return v;
        }
        if (QUERY_FIELD_DOUBLE.contains(new StringBuilder().append(",").append(f).append(",").toString()))
            v = "0";
        else if (QUERY_FIELD_TIMESTAMP.contains(new StringBuilder().append(",").append(f).append(",").toString())) {
            if ((StringUtils.isEmpty(v)) || ("null".equals(v)))
                v = "1970-01-01 08:00:00";
        }
        else {
            v = " ";
        }
        return v;
    }

    public static String parseProdcutField(String v, String f, String QUERY_FIELD_DOUBLE, String QUERY_FIELD_TIMESTAMP)
    {
        if ((StringUtils.isEmpty(v)) || (v.equals("null"))) {
            if (QUERY_FIELD_DOUBLE.contains(new StringBuilder().append(",").append(f).append(",").toString()))
                v = "0";
            else if (QUERY_FIELD_TIMESTAMP.contains(new StringBuilder().append(",").append(f).append(",").toString())) {
                if ((StringUtils.isEmpty(v)) || ("null".equals(v)))
                    v = "1970-01-01 08:00:00";
            }
            else {
                v = " ";
            }
        }
        if (QUERY_FIELD_TIMESTAMP.contains(new StringBuilder().append(",").append(f).append(",").toString())) {
            v = DateUtil.getNotRuleTimeStr(v);
        }
        return v;
    }

    public static Map<String, Map<String, String>> parse2MapMapString(Map<String, Map<String, Set<String>>> subMap)
    {
        String venderId;
        Map<String,Set<String>> ms;
        Map map = new HashMap();
        Iterator i$;
        if (!subMap.isEmpty())
            for (i$ = subMap.keySet().iterator(); i$.hasNext(); ) {
                venderId = (String)i$.next();
                ms = (Map)subMap.get(venderId);
                for (String activityID : ms.keySet()) {
                    String subsStr = "";
                    Set<String> partnumberSet = (Set)ms.get(activityID);
                    for (String sp : partnumberSet) {
                        subsStr = new StringBuilder().append(sp).append(",").append(subsStr).toString();
                    }
                    if (StringUtils.isNotEmpty(subsStr)) {
                        subsStr = subsStr.substring(0, subsStr.length() - 1);
                        addItem2MapMap(map, venderId, activityID, subsStr);
                    }
                }
            }

        return map;
    }

    public static void parse2MapMapString(Map<String, Map<String, Set<String>>> subMap, Map<String, Map<String, String>> map)
    {
        String venderId;
        Map<String,Set<String>> ms;
        Iterator i$;
        if ((subMap != null) && (!subMap.isEmpty()))
            for (i$ = subMap.keySet().iterator(); i$.hasNext(); ) {
                venderId = (String)i$.next();
                ms = (Map)subMap.get(venderId);
                for (String activityID : ms.keySet()) {
                    String subsStr = "";
                    Set<String> partnumberSet = (Set)ms.get(activityID);
                    for (String sp : partnumberSet) {
                        subsStr = new StringBuilder().append(sp).append(",").append(subsStr).toString();
                    }
                    if (StringUtils.isNotEmpty(subsStr)) {
                        subsStr = subsStr.substring(0, subsStr.length() - 1);
                        addItem2MapMap(map, venderId, activityID, subsStr);
                    }
                }
            }
    }

    public static void parse2MapMapString2(Map<String, Map<String, Set<String>>> subMap, Map<String, Map<String, String>> map)
    {
        String venderId;
        Map<String,Set<String>> ms;
        Iterator i$;
        if ((subMap != null) && (!subMap.isEmpty()))
            for (i$ = subMap.keySet().iterator(); i$.hasNext(); ) {
                venderId = (String)i$.next();
                ms = (Map)subMap.get(venderId);
                for (String activityID : ms.keySet()) {
                    String subsStr = "";
                    Set<String> partnumberSet = (Set)ms.get(activityID);
                    for (String sp : partnumberSet) {
                        subsStr = new StringBuilder().append(sp).append("#").append(subsStr).toString();
                    }
                    if (StringUtils.isNotEmpty(subsStr)) {
                        subsStr = subsStr.substring(0, subsStr.length() - 1);
                        addItem2MapMap(map, venderId, activityID, subsStr);
                    }
                }
            }

    }

    public static void addItem2MapMap(Map<String, Map<String, String>> jsonMap, String key, String field, String value)
    {
        if (jsonMap.get(key) == null) {
            Map<String,String> obj = new HashMap();
            obj.put(field, value);
            jsonMap.put(key, obj);
        } else {
            ((Map)jsonMap.get(key)).put(field, value);
        }
    }

    public static void addItem2MapMap(Map<String, Map<String, String>> jsonMap, String key, String field, String value, String splitStr)
    {
        Map map = (Map)jsonMap.get(key);
        if (map == null) {
            Map obj = new HashMap();
            obj.put(field, value);
            jsonMap.put(key, obj);
        } else {
            String v = (String)map.get(field);
            if (StringUtils.isEmpty(v))
                map.put(field, value);
            else
                map.put(field, new StringBuilder().append(value).append(splitStr).append(v).toString());
        }
    }

    public static void addItem2Map(Map<String, String> jsonMap, String key, String value)
    {
        if (jsonMap.get(key) == null) {
            jsonMap.put(key, value);
        } else {
            String v = (String)jsonMap.get(key);
            jsonMap.put(key, new StringBuilder().append(v).append("#").append(value).toString());
        }
    }

    public static String addSupplier(String supplierCode)
    {
        supplierCode = StringUtils.trim(supplierCode);
        if (supplierCode == null) {
            return null;
        }
        if (supplierCode.length() == 8) {
            return new StringBuilder().append("00").append(supplierCode).toString();
        }
        return supplierCode;
    }

    public static void addItem2MapList(Map<String, List<String>> jsonMap, String key, String value)
    {
        if (jsonMap.get(key) == null) {
            List set = new ArrayList();
            set.add(value);
            jsonMap.put(key, set);
        } else {
            ((List)jsonMap.get(key)).add(value);
        }
    }

    public static void addItem2MapSet(Map<String, Set<String>> jsonMap, String key, String value)
    {
        if (jsonMap.get(key) == null) {
            Set set = new HashSet();
            set.add(value);
            jsonMap.put(key, set);
        } else {
            ((Set)jsonMap.get(key)).add(value);
        }
    }

    public static void addItem2MapSortedSet(Map<String, Set<String>> jsonMap, String key, String value)
    {
        if (jsonMap.get(key) == null) {
            Set set = new TreeSet();
            set.add(value);
            jsonMap.put(key, set);
        } else {
            ((Set)jsonMap.get(key)).add(value);
        }
    }

    public static void setMapMapSet(String subs, String vendor, String activityID, Map<String, Map<String, Set<String>>> subMap)
    {
        if (StringUtils.isBlank(subs)) {
            return;
        }
        String[] sa = subs.split(",");
        if (subMap.get(vendor) == null) {
            Map ms = new HashMap();
            Set set = new HashSet();
            for (String ss : sa) {
                set.add(ss);
            }
            ms.put(activityID, set);
            subMap.put(vendor, ms);
        } else {
            Map ms = (Map)subMap.get(vendor);
            if (ms.get(activityID) == null) {
                Set set = new HashSet();
                for (String ss : sa) {
                    set.add(ss);
                }
                ((Map)subMap.get(vendor)).put(activityID, set);
            } else {
                for (String ss : sa)
                    ((Set)((Map)subMap.get(vendor)).get(activityID)).add(ss);
            }
        }
    }

    public static void setMapMapSet(String[] sa, String vendor, String activityID, Map<String, Map<String, Set<String>>> subMap)
    {
        if (sa == null) {
            return;
        }
        if (subMap.get(vendor) == null) {
            Map ms = new HashMap();
            Set set = new HashSet();
            for (String ss : sa) {
                set.add(ss);
            }
            ms.put(activityID, set);
            subMap.put(vendor, ms);
        } else {
            Map ms = (Map)subMap.get(vendor);
            if (ms.get(activityID) == null) {
                Set set = new HashSet();
                for (String ss : sa) {
                    set.add(ss);
                }
                ((Map)subMap.get(vendor)).put(activityID, set);
            } else {
                for (String ss : sa)
                    ((Set)((Map)subMap.get(vendor)).get(activityID)).add(ss);
            }
        }
    }

    public static String getChannel(String channel)
    {
        String str = "";
        if ("31".equals(channel))
            str = "PC";
        else if ("32".equals(channel))
            str = "APP";
        else if ("36".equals(channel)) {
            str = "WAP";
        }
        return str;
    }

    public static Map<String, Integer> getActivityTypeMap()
    {
        Map activityTypeMap = new HashMap();
        activityTypeMap.put("1", Integer.valueOf(8));
        activityTypeMap.put("2", Integer.valueOf(9));
        activityTypeMap.put("3", Integer.valueOf(10));
        activityTypeMap.put("4", Integer.valueOf(11));
        activityTypeMap.put("5", Integer.valueOf(12));
        activityTypeMap.put("6", Integer.valueOf(13));
        activityTypeMap.put("14", Integer.valueOf(13));
        activityTypeMap.put("7", Integer.valueOf(16));
        activityTypeMap.put("8", Integer.valueOf(0));
        activityTypeMap.put("9", Integer.valueOf(0));
        activityTypeMap.put("10", Integer.valueOf(9));
        activityTypeMap.put("23", Integer.valueOf(23));
        activityTypeMap.put("112", Integer.valueOf(24));
        return activityTypeMap;
    }

    public static Map<String, String> getChannelMap()
    {
        Map codeMap = new HashMap();
        codeMap.put("31", "pc");
        codeMap.put("32", "mobile");
        codeMap.put("36", "wap");
        codeMap.put("all", "all");
        codeMap.put("pc", "pc");
        codeMap.put("wap", "wap");
        codeMap.put("mobile", "mobile");
        codeMap.put("tv", "tv");
        codeMap.put("rby", "rby");
        codeMap.put("pad", "pad");
        return codeMap;
    }

    public static String shrtCityCode(String cityCode)
    {
        if (StringUtils.isEmpty(cityCode)) {
            return "";
        }
        if (cityCode.length() == 12) {
            cityCode = cityCode.replaceFirst("00000", "");
        }
        return cityCode;
    }

    public static String getNinePartnumber(String partnumber)
    {
        if (StringUtils.isEmpty(partnumber)) {
            return partnumber;
        }
        partnumber = partnumber.trim();
        String _partnumber = partnumber.replaceAll("^(0+)", "");
        return _partnumber;
    }

    public static String getUnZeroPartnumber(String partnumber)
    {
        if (StringUtils.isEmpty(partnumber)) {
            return partnumber;
        }
        partnumber = partnumber.trim();
        String _partnumber = partnumber.replaceAll("^(0+)", "");
        return _partnumber;
    }

    public static String getMdmCity(String city)
    {
        if (StringUtils.isEmpty(city)) {
            return city;
        }
        if (city.length() == 12) {
            return city.substring(5);
        }
        return city;
    }

    public static String getVendr(String vendor)
    {
        if (StringUtils.isEmpty(vendor)) {
            return vendor;
        }
        if (vendor.length() == 8) {
            return new StringBuilder().append("00").append(vendor).toString();
        }
        return vendor;
    }

    public static String getEightVendr(String vendor)
    {
        if (StringUtils.isEmpty(vendor)) {
            return vendor;
        }
        if (vendor.length() == 8) {
            return vendor;
        }
        if (vendor.length() == 10) {
            return vendor.substring(2);
        }
        return vendor;
    }

    public static String getJsonString(Object lineItem) {
        String str = "";
        JSONObject jsonObj = null;
        try {
            jsonObj = JSONObject.fromObject(lineItem);
        } catch (Exception e) {
            logger.error("数据转换错误：", e);
        }
        if (jsonObj != null) {
            str = jsonObj.toString();
        }
        return str;
    }

    public static String getNodeValue(JSONObject obj, String key)
    {
        String str = "";
        try {
            return obj.getString(key);
        }
        catch (Exception e) {
            str = "";
        }
        return str;
    }

    public static int calShardNum(String partnumber)
    {
        int shardHashValue = partnumber.hashCode() & 0x7FFFFFFF;
        int solrShard = shardHashValue % 84 + 10;
        return solrShard;
    }

    public static Map<String, Object> parseJsonString(String jsonString)
    {
        JSONObject jsonObject = null;
        try {
            jsonObject = JSONObject.fromObject(jsonString);
        } catch (Exception e) {
            logger.info(new StringBuilder().append("解析数据出错").append(jsonString).toString(), e);
        }

        Map resultMap = (Map)getObject(jsonObject);
        return resultMap;
    }

    public static Map<String, Object> getJsonMap(String jsonString)
    {
        if (StringUtils.isBlank(jsonString)) {
            return new HashMap();
        }
        Map obj = null;
        try {
            obj = parseJsonString(jsonString);
        } catch (Exception e) {
            logger.error("解析报文出错", e);
        }
        if (obj == null) {
            obj = new HashMap();
        }
        return obj;
    }

    public static Object getObject(Object obj)
    {
        if ((obj instanceof JSONObject)) {
            JSONObject jsonObj = (JSONObject)obj;
            Map map = new HashMap();
            for (Iterator i$ = jsonObj.keySet().iterator(); i$.hasNext(); ) { Object key = i$.next();
                map.put(new StringBuilder().append(key).append("").toString(), getObject(jsonObj.get(key)));
            }
            return map;
        }if ((obj instanceof JSONArray)) {
        JSONArray array = (JSONArray)obj;
        List list = new ArrayList();
        for (int i = 0; i < array.size(); i++) {
            Object ele = array.get(i);
            list.add(getObject(ele));
        }
        return list;
    }
        return obj;
    }

    public static Object getSortObject(Object obj)
    {
        if ((obj instanceof JSONObject)) {
            JSONObject jsonObj = (JSONObject)obj;
            Map map = new TreeMap();
            for (Iterator i$ = jsonObj.keySet().iterator(); i$.hasNext(); ) { Object key = i$.next();
                map.put(new StringBuilder().append(key).append("").toString(), getObject(jsonObj.get(key)));
            }
            return map;
        }if ((obj instanceof JSONArray)) {
        JSONArray array = (JSONArray)obj;
        List list = new ArrayList();
        for (int i = 0; i < array.size(); i++) {
            Object ele = array.get(i);
            list.add(getObject(ele));
        }
        return list;
    }
        return obj;
    }

    public static String subBrandCode(String brandCode)
    {
        if ((null == brandCode) || (brandCode.length() < 4)) {
            return "null";
        }
        String brandId = brandCode.substring(brandCode.length() - 4);
        return brandId;
    }

    public static String getJsonValue(JSONObject jo, String key) {
        if (jo == null) {
            return "";
        }
        String v = "";
        try {
            v = String.valueOf(jo.get(key));
            if (StringUtils.isEmpty(v))
                v = "";
            else
                v = StringUtils.trim(v);
        }
        catch (Exception e) {
            logger.error("解析JSON数据出错:", e);
        }
        if ((v == null) || ("null".equals(v))) {
            v = "";
        }
        return v;
    }

    public static void callServer(List<String> servletNames)
    {
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(20, 30, 10L, TimeUnit.SECONDS, new ArrayBlockingQueue(100));

        for (String servletName : servletNames) {
            CallServer thread = new CallServer();
            thread.url = servletName;
            threadPool.execute(thread);
        }
        shutdownPool(threadPool);
    }

    public static void callServer(String[] servletNames)
    {
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(20, 30, 10L, TimeUnit.SECONDS, new ArrayBlockingQueue(100));

        for (String servletName : servletNames) {
            CallServer thread = new CallServer();
            thread.url = servletName;
            threadPool.execute(thread);
        }
        shutdownPool(threadPool);
    }

    public static void shutdownPool(ThreadPoolExecutor producerPool)
    {
        while (!producerPool.isShutdown()) {
            if (producerPool.getActiveCount() == 0)
                producerPool.shutdown();
            try
            {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                logger.error(new StringBuilder().append("关闭线程连接池报错").append(e.getMessage()).toString(), e);
            }
        }
    }

    public static String DB2_Replace(String str, String searchString, String replacement) {
        if (StringUtils.isBlank(str)) {
            return "";
        }
        if (str.isEmpty()) {
            return str;
        }
        return StringUtils.replace(str, searchString, replacement);
    }

    public static String DB2_RTrim(String str)
    {
        if (StringUtils.isBlank(str)) {
            return "";
        }
        if (str.isEmpty()) {
            return str;
        }
        if (str.charAt(str.length() - 1) != ' ') {
            return str;
        }

        int n = str.length() - 2;
        while ((n >= 0) &&
                (str.charAt(n) == ' ')) {
            n--;
        }

        return str.substring(0, n + 1);
    }

    public static String DB2_Hex(String str) {
        if (StringUtils.isBlank(str)) {
            return "";
        }
        if (str.isEmpty()) {
            return str;
        }
        byte[] binData;
        try
        {
            binData = str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        char[] chars = new char[binData.length * 2];
        for (int i = 0; i < binData.length; i++) {
            int n = binData[i];
            if (n < 0) {
                n += 256;
            }
            chars[(i * 2)] =StaticStatus.DB2_HEX_CHARS[(n / 16)];
            chars[(i * 2 + 1)] = StaticStatus.DB2_HEX_CHARS[(n % 16)];
        }
        return new String(chars);
    }
    public static JSONObject getJsonObj(String msg) {
        JSONObject jsonObject = null;
        try {
            jsonObject = JSONObject.fromObject(msg);
        } catch (Exception e) {
            logger.info(new StringBuilder().append("解析数据出错").append(msg).toString(), e);
        }
        return jsonObject;
    }

    public static String getJsonVal(JSONObject jsonObject, String key) {
        String value = "";
        try {
            if (jsonObject.containsKey(key))
                value = jsonObject.getString(key);
            else if (jsonObject.containsKey(key.toUpperCase()))
                value = jsonObject.getString(key.toUpperCase());
        }
        catch (Exception e) {
            logger.info(new StringBuilder().append("取JSONObject数据出错=").append(key).toString(), e);
        }
        if ((value == null) || (StringUtils.isEmpty(value)) || ("null".equals(value))) {
            value = "";
        }
        return value;
    }

    public static String requestUrl(String url)
    {
        String returnVal = "ERROR";

        HttpClient client = new HttpClient();
        client.getHttpConnectionManager().getParams().setConnectionTimeout(20000);

        client.getHttpConnectionManager().getParams().setSoTimeout(20000);
        GetMethod postMethod = new GetMethod(url);
        try
        {
            int statusCode = client.executeMethod(postMethod);
            if (statusCode != 200) {
                logger.error(new StringBuilder().append("服务器出错，请求POST方法，返回出错！").append(postMethod.getStatusLine()).toString());
            }

            byte[] responseBody = postMethod.getResponseBody();
            returnVal = new String(responseBody, postMethod.getResponseCharSet());
        }
        catch (Exception e) {
            returnVal = "ERROR";
            logger.error(new StringBuilder().append("请求").append(url).append(";返回出错！").append(e.getMessage()).toString(), e);
        } finally {
            postMethod.releaseConnection();

            client.getHttpConnectionManager().closeIdleConnections(0L);
        }
        return returnVal;
    }

    public static String replaceTagA(String msg)
    {
        msg = StringUtils.trimToEmpty(msg);
        if (StringUtils.isNotEmpty(msg)) {
            msg = msg.replaceAll("<[^>].*?\\/[\\w]+>", "");
        }
        return msg;
    }

    public static boolean isNotBlankAndNull(String str)
    {
        return (!"null".equalsIgnoreCase(str)) && (StringUtils.isNotBlank(str));
    }

    public static Map<String, Map<String, String>> sortMapByKey(Map<String, Map<String, String>> map)
    {
        if ((map == null) || (map.isEmpty())) {
            return new TreeMap();
        }
        Map sortMap = new TreeMap(new MyKeyComparator());
        sortMap.putAll(map);
        return sortMap;
    }

    public static String getJsonKeyVal(String val)
    {
        String rusult = "[]";
        try {
            if (StringUtils.isNotBlank(val)) {
                JSONObject jsonobject = JSONObject.fromObject(val);
                if (jsonobject != null) {
                    if (!jsonobject.containsKey("key")) return rusult;
                    JSONArray jsonarray = jsonobject.getJSONArray("key");
                    return jsonarray.toString();
                }
            }
        }
        catch (Exception e) {
        }
        return rusult;
    }

    public static String getKeyVal(String val)
    {
        String rusult = "";
        try {
            if (StringUtils.isNotBlank(val)) {
                JSONObject jsonobject = JSONObject.fromObject(val);
                if (jsonobject != null) {
                    if (!jsonobject.containsKey("key")) return rusult;
                    JSONArray jsonarray = jsonobject.getJSONArray("key");
                    int len = jsonarray.size();
                    StringBuilder stringbuilder = new StringBuilder();
                    for (int i = 0; i < len; i++) {
                        stringbuilder.append(new StringBuilder().append(jsonarray.get(i)).append("").append(",").toString());
                    }
                    String str = stringbuilder.toString();
                    int strLen = str.length();
                    if (strLen > 0)
                        rusult = str.substring(0, str.length() - 1);
                }
            }
        }
        catch (Exception e)
        {
        }
        return rusult;
    }

    public static String virtualSplit(String virtual_Name)
    {
        String virtualFianl = "";
        if (StringUtils.isNotBlank(virtual_Name)) {
            String[] vns = StringUtils.split(virtual_Name, " ,[]【】()（）");
            Set vnSet = new HashSet(Arrays.asList(vns));
            virtualFianl = StringUtils.join(vnSet, " ");
        }
        return virtualFianl;
    }

    public static String sortPromotionStr(String compareValue)
    {
        if (StringUtils.isEmpty(compareValue)) {
            return "";
        }
        String[] subs = compareValue.split("#");
        List<String> list = Arrays.asList(subs);
        Collections.sort(list);
        String str = "";
        for (String s : list) {
            str = new StringBuilder().append(str).append("#").append(s).toString();
        }
        return str;
    }

    public static String sortJsonStr(String compareValue)
    {
        String txt = "";
        try
        {
            Map sortMap = new TreeMap();
            if ((StringUtils.isNotBlank(compareValue)) && (!"null".equals(compareValue))) {
                JSONObject jsonObj = JSONObject.fromObject(compareValue);

                Iterator it = jsonObj.entrySet().iterator();
                while (it.hasNext())
                {
                    Map.Entry m = (Map.Entry)it.next();
                    sortMap.put(m.getKey(), m.getValue());
                }
            }
            txt = JSONObject.fromObject(sortMap).toString();
        } catch (Exception e) {
            logger.error("json格式错误", e);
            txt = compareValue;
        }
        return txt;
    }

    public static long parseLong(String timeNo)
    {
        long curVersionTime = 0L;
        try {
            curVersionTime = Long.parseLong(timeNo);
        } catch (Exception e) {
            logger.error("json格式错误", e);
        }
        return curVersionTime;
    }

    public static String parseCatalog(String catalogStr) {
        String catalogValue = "";
        String[] catalogArray = catalogStr.split(",");
        int length = catalogArray.length;
        for (int i = 0; i < length; i++) {
            boolean flag = false;
            if (StringUtils.isNotBlank(catalogArray[i])) {
                catalogValue = new StringBuilder().append(catalogValue).append(catalogArray[i]).toString();
                flag = true;
            }
            if ((flag) && (i != length - 1)) {
                catalogValue = new StringBuilder().append(catalogValue).append(",").toString();
            }
        }
        return catalogValue;
    }

    public static String reverse(String str)
    {
        return new StringBuilder(str).reverse().toString();
    }

    public static boolean checkAttrLine(String str1, String str2) {
        if ((StringUtils.isBlank(str1)) && (StringUtils.isBlank(str2))) {
            return true;
        }
        if (StringUtils.equals(str1, str2))
            return true;
        try
        {
            String[] arrs1 = str1.split(",");
            String[] arrs2 = str2.split(",");
            if (arrs1.length != arrs2.length)
            {
                return false;
            }
            if ((!StringUtils.equals(arrs1[0], arrs2[0])) || (!StringUtils.equals(arrs1[1], arrs2[1])) || (!StringUtils.equals(arrs1[2], arrs2[2])))
            {
                return false;
            }
            Set set1 = new HashSet(Arrays.asList(StringUtils.splitByWholeSeparator(unGzipString(arrs1[3]), "@@")));
            Set set2 = new HashSet(Arrays.asList(StringUtils.splitByWholeSeparator(unGzipString(arrs2[3]), "@@")));
            boolean avs = SetUtils.isEqualSet(set1, set2);
            boolean ravs = true;
            if (arrs1.length > 4) {
                Set set3 = new HashSet(Arrays.asList(StringUtils.splitByWholeSeparator(unGzipString(arrs1[4]), "@@")));
                Set set4 = new HashSet(Arrays.asList(StringUtils.splitByWholeSeparator(unGzipString(arrs2[4]), "@@")));
                ravs = SetUtils.isEqualSet(set3, set4);
            }
            if ((avs) && (ravs)) {
                return true;
            }
        }
        catch (Exception e)
        {
            return false;
        }
        return false;
    }

    public static List<String> getChannels(String channelS) {
        List list = new ArrayList();
        String[] array = channelS.split("\t");
        for (String chanel : array) {
            if ((!StringUtils.isBlank(chanel)) && (chanel.length() >= 3))
            {
                if ('1' == chanel.charAt(0)) {
                    list.add("PC");
                }
                if ('1' == chanel.charAt(1)) {
                    list.add("APP");
                }
                if ('1' == chanel.charAt(2))
                    list.add("WAP");
            }
        }
        return list;
    }
}
