package cn.suning.hadoop.job.job1;


import cn.suning.hadoop.HdfsUtils;
import cn.suning.hadoop.job.base.BaseReducer;
import com.alibaba.fastjson.JSONObject;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class DataReduce extends BaseReducer
{
    private static Logger logger = Logger.getLogger(DataReduce.class);

    private Map<String, List<Map<String, Object>>> attrAlias = new HashMap();
    private Map<String, String> paramInfo = new HashMap();
    private Map<String, Map<String, Object>> valueAliasCache = new HashMap();
    private Map<String, String> selectCache = new HashMap();
    private Map<String, List<Map<String, Object>>> rangeCache = new HashMap();
    private Map<String, String> appVACache = new HashMap();

    private Pattern pat = null;

    private void initAV() {
        String attrAliasPath = "/user/sousuo/data/das/common/sparkGoodsAttr/goodsAttrAliasCollect/full/";
        try {
            Path folder = new Path(HdfsUtils.getAllPath(this.fs, attrAliasPath, 4));
            FileStatus[] fss = this.fs.listStatus(folder);
            if (fss != null) {
                for (FileStatus filestatus : fss) {
                    Path path = filestatus.getPath();
                    if ((filestatus.isFile()) && (!"_SUCCESS".equals(path.getName()))) {
                        FSDataInputStream fis = this.fs.open(path, 1024);
                        BufferedReader bis = new BufferedReader(new InputStreamReader(fis));
                        String line = null;
                        while ((line = bis.readLine()) != null) {
                            JSONObject jo = JSONObject.parseObject(line);
                            String pc = jo.getString("PARAM_CODE");
                            String ca = jo.getString("CAT_CODE");
                            String key = pc + "_" + ca;
                            List aas = (List)this.attrAlias.get(key);
                            if (aas == null) {
                                aas = new ArrayList();
                                this.attrAlias.put(key, aas);
                            }
                            aas.add(jo);
                        }
                    }
                }
            }
            logger.info("DataReduce(" + this + ")初始化,attrAlias共" + this.attrAlias.size() + "条数据！");
        } catch (Exception e) {
            e.printStackTrace();
        } }

    private void initParam() {
        String paramPath = "/user/sousuo/data/das/common/sparkGoodsAttr/goodParamCollect/full/";
        try {
            Path folder = new Path(HdfsUtils.getAllPath(this.fs, paramPath, 4));
            FileStatus[] fss = this.fs.listStatus(folder);
            if (fss != null) {
                for (FileStatus filestatus : fss) {
                    Path path = filestatus.getPath();
                    if ((filestatus.isFile()) && (!"_SUCCESS".equals(path.getName()))) {
                        FSDataInputStream fis = this.fs.open(path, 1024);
                        BufferedReader bis = new BufferedReader(new InputStreamReader(fis));
                        String line = null;
                        while ((line = bis.readLine()) != null) {
                            JSONObject jo = JSONObject.parseObject(line);
                            String key = jo.getString("PRMT_CODE");
                            if (StringUtils.isNotBlank(key)) {
                                String pvim = jo.getString("PRMT_VALUE_INPUT_MEANS");
                                String dt = jo.getString("DATA_TYPE");
                                if (("1".equals(pvim)) || ("2".equals(pvim)))
                                    this.paramInfo.put(key, "type2");
                                else if (("3".equals(pvim)) && ("NUM".equals(dt))) {
                                    this.paramInfo.put(key, "type1");
                                }
                                else if ((!"3".equals(pvim)) || (!"CHAR".equals(dt)));
                            }
                        }
                    }
                }

            }

            logger.info("DataReduce(" + this + ")初始化,paramInfo共" + this.paramInfo.size() + "条数据！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initParamSelect() { String selectPath = "/user/sousuo/data/das/common/sparkGoodsAttr/goodsSelectCollect/full/";
        try {
            Path folder = new Path(HdfsUtils.getAllPath(this.fs, selectPath, 4));
            FileStatus[] fss = this.fs.listStatus(folder);
            if (fss != null) {
                for (FileStatus filestatus : fss) {
                    Path path = filestatus.getPath();
                    if ((filestatus.isFile()) && (!"_SUCCESS".equals(path.getName()))) {
                        FSDataInputStream fis = this.fs.open(path, 1024);
                        BufferedReader bis = new BufferedReader(new InputStreamReader(fis));
                        String line = null;
                        while ((line = bis.readLine()) != null) {
                            JSONObject jo = JSONObject.parseObject(line);
                            String key = jo.getString("PRMT_CODE") + "_" + jo.getString("PRMT_VALUE_CODE");
                            this.selectCache.put(key, jo.getString("PRMT_DESC"));
                        }
                    }
                }
            }
            logger.info("DataReduce(" + this + ")初始化,selectCache共" + this.selectCache.size() + "条数据！");
        } catch (Exception e) {
            e.printStackTrace();
        } }

    private void initAVA() {
        String avaPath = "/user/sousuo/data/das/common/sparkGoodsAttr/goodsAttrValueAliasCollect/full/";
        try {
            Path folder = new Path(HdfsUtils.getAllPath(this.fs, avaPath, 4));
            FileStatus[] fss = this.fs.listStatus(folder);
            if (fss != null) {
                for (FileStatus filestatus : fss) {
                    Path path = filestatus.getPath();
                    if ((filestatus.isFile()) && (!"_SUCCESS".equals(path.getName()))) {
                        FSDataInputStream fis = this.fs.open(path, 1024);
                        BufferedReader bis = new BufferedReader(new InputStreamReader(fis));
                        String line = null;
                        while ((line = bis.readLine()) != null) {
                            JSONObject jo = JSONObject.parseObject(line);
                            String key = jo.getString("CAT_CODE") + "_" + jo.getString("PARAM_CODE") + "_" + jo.getString("PARAM_VALUE");
                            if (StringUtils.isNotBlank(key)) {
                                this.valueAliasCache.put(key, jo);
                            }
                        }
                    }
                }
            }
            logger.info("DataReduce(" + this + ")初始化,valueAliasCache共" + this.valueAliasCache.size() + "条数据！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initRange() { String rangePath = "/user/sousuo/data/das/common/sparkGoodsAttr/goodsAttrRangeCollect/full/";
        try {
            Path folder = new Path(HdfsUtils.getAllPath(this.fs, rangePath, 4));
            FileStatus[] fss = this.fs.listStatus(folder);
            if (fss != null) {
                for (FileStatus filestatus : fss) {
                    Path path = filestatus.getPath();
                    if ((filestatus.isFile()) && (!"_SUCCESS".equals(path.getName()))) {
                        FSDataInputStream fis = this.fs.open(path, 1024);
                        BufferedReader bis = new BufferedReader(new InputStreamReader(fis));
                        String line = null;
                        while ((line = bis.readLine()) != null) {
                            JSONObject jo = JSONObject.parseObject(line);
                            String key = jo.getString("ATTR_CODE") + "_" + jo.getString("CAT_CODE");
                            List ranges = (List)this.rangeCache.get(key);
                            if (ranges == null) {
                                ranges = new ArrayList();
                                this.rangeCache.put(key, ranges);
                            }
                            ranges.add(jo);
                        }
                    }
                }
            }
            logger.info("DataReduce(" + this + ")初始化,rangeCache共" + this.rangeCache.size() + "条数据！");
        } catch (Exception e) {
            e.printStackTrace();
        } }

    public void initAppVA() {
        String appVAPath = "/user/sousuo/data/das/common/sparkGoodsAttr/tfAvaAppRelCollect/full/";
        try {
            Path folder = new Path(HdfsUtils.getAllPath(this.fs, appVAPath, 4));
            FileStatus[] fss = this.fs.listStatus(folder);
            if (fss != null) {
                for (FileStatus filestatus : fss) {
                    Path path = filestatus.getPath();
                    if ((filestatus.isFile()) && (!"_SUCCESS".equals(path.getName()))) {
                        FSDataInputStream fis = this.fs.open(path, 1024);
                        BufferedReader bis = new BufferedReader(new InputStreamReader(fis));
                        String line = null;
                        while ((line = bis.readLine()) != null) {
                            JSONObject jo = JSONObject.parseObject(line);
                            String key = jo.getString("PARAM_CODE") + "_" + jo.getString("CAT_CODE") + "_" + jo.getString("ATTR_VALUE");
                            if (StringUtils.isNotBlank(key)) {
                                this.appVACache.put(key, jo.getString("APP_ATTR_VALUE"));
                            }
                        }
                    }
                }
            }
            logger.info("DataReduce(" + this + ")初始化,appVACache共" + this.appVACache.size() + "条数据！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<String, Object> toGether(Map<String, Object> gc, Map<String, Object> gp, Map<String, Object> ad) {
        Map record = new HashMap();
        record.put("CMMDTY_CODE", gc.get("CMMDTY_CODE"));
        record.put("TEMP_CAT_CODE", gc.get("SALE_CATALOG"));

        record.put("ATTR_CODE", ad.get("ATTR_CODE"));
        record.put("ATTR_NAME", ad.get("ATTR_NAME"));
        record.put("TEMP_RELA_TYPE", ad.get("RELA_TYPE"));

        record.put("TEMP_PRMT_OPTION_CODE", gp.get("PRMT_OPTION_CODE"));
        record.put("PRMT_CODE", gp.get("PRMT_CODE"));
        record.put("TEMP_PRMT_DESC", gp.get("PRMT_DESC"));
        record.put("PRMT_VALUE", gp.get("PRMT_VALUE"));
        return record;
    }
    public Map<String, Object> otherJoin(String partnumber, String attrCode, List<Map<String, Object>> datas) {
        Map resultMap = new LinkedHashMap();
        String attrName = null;
        for (int i = datas.size() - 1; i >= 0; i--) {
            Map data = (Map)datas.get(i);

            String pc = (String)data.get("PRMT_CODE");
            String paramType = (String)this.paramInfo.get(pc);
            if (paramType != null) {
                if ("type2".equals(paramType)) {
                    String selectKey = data.get("PRMT_CODE") + "_" + data.get("TEMP_PRMT_OPTION_CODE");
                    String selectValue = (String)this.selectCache.get(selectKey);
                    if (selectValue != null) {
                        data.put("PRMT_VALUE", selectValue);
                    } else {
                        data.put("步骤", Integer.valueOf(1));
                        datas.remove(i);
                        continue;
                    }
                } else {
                    data.put("PRMT_VALUE", data.get("PRMT_VALUE"));
                }

                String avaKey = data.get("TEMP_CAT_CODE") + "_" + data.get("PRMT_CODE") + "_" + data.get("PRMT_VALUE");
                Map vav = (Map)this.valueAliasCache.get(avaKey);
                if (vav != null) {
                    if ("1".equals(vav.get("IF_COLLECT"))) {
                        String av = (String)vav.get("ATTR_VALUE");
                        if (StringUtils.isNotBlank(av))
                            data.put("ATTR_VALUE", av);
                        else
                            data.put("ATTR_VALUE", data.get("TEMP_PRMT_DESC"));
                    }
                    else {
                        data.put("步骤", Integer.valueOf(2));
                        datas.remove(i);
                        continue;
                    }
                }
                else data.put("ATTR_VALUE", data.get("PRMT_VALUE"));

                String av = (String)data.get("ATTR_VALUE");
                if (StringUtils.isBlank(av)) {
                    data.put("ATTR_VALUE", data.get("PRMT_VALUE"));
                }

                String rangeKey = data.get("ATTR_CODE") + "_" + data.get("TEMP_CAT_CODE");
                List<Map<String,Object>> rvs = (List)this.rangeCache.get(rangeKey);
                if (rvs != null) {
                    String paramValue = (String)data.get("PRMT_VALUE");
                    if (StringUtils.isNotBlank(paramValue)) {
                        List<String> avrds = new ArrayList();
                        for (Map<String,Object> rv : rvs) {
                            String avrd = (String)rv.get("ATTR_VALUE_RANGE_DESC");
                            String avmin = (String)rv.get("ATTR_VALUE_MIN");
                            String avmax = (String)rv.get("ATTR_VALUE_MAX");
                            try {
                                Matcher mat = this.pat.matcher(paramValue);
                                if (mat.find()) {
                                    String paramValueNumber = mat.group(1);
                                    if ((Float.valueOf(avmin).floatValue() <= Float.valueOf(paramValueNumber).floatValue()) && (Float.valueOf(paramValueNumber).floatValue() <= Float.valueOf(avmax).floatValue()))
                                    {
                                        avrds.add(avrd);
                                    }
                                }
                            } catch (NumberFormatException e) {
                                logger.info("属性范围值取值出错" + partnumber + "-" + attrCode);
                            }
                        }
                        if (!avrds.isEmpty()) {
                            data.put("ATTR_VALUE_RANGE", avrds);
                        }
                    }
                }

                String appVAKey = data.get("PRMT_CODE") + "_" + data.get("TEMP_CAT_CODE") + "_" + data.get("ATTR_VALUE");
                String appVAValue = (String)this.appVACache.get(appVAKey);
                if (appVAValue == null) {
                    data.put("APP_ATTR_VALUE", appVAValue);
                }
                if (attrName == null)
                    attrName = (String)data.get("ATTR_NAME");
            }
            else {
                data.put("步骤", Integer.valueOf(0));
                datas.remove(i);
            }

        }

        if (datas.isEmpty()) {
            return null;
        }

        resultMap.put("CMMDTY_CODE", partnumber);
        if ((StringUtils.isBlank(attrCode)) || (StringUtils.isBlank(attrName))) {
            return null;
        }
        resultMap.put("ATTR_CODE", attrCode);
        resultMap.put("ATTR_NAME", attrName);
        Set attrValues = new LinkedHashSet();
        Set appValues = new LinkedHashSet();
        Set rangeValues = new LinkedHashSet();
        for (Map data : datas) {
            attrValues.add((String)data.get("ATTR_VALUE"));

            String appAV = (String)data.get("APP_ATTR_VALUE");
            if (StringUtils.isNotBlank(appAV)) {
                appValues.add(appAV);
            }

            List avrs = (List)data.get("ATTR_VALUE_RANGE");
            if (avrs != null) {
                rangeValues.addAll(avrs);
            }
        }
        String value = StringUtils.join(attrValues, "@@");
        if (!appValues.isEmpty()) {
            value = value + "&app>" + StringUtils.join(attrValues, "@@");
        }
        resultMap.put("ATTR_VALUE", value);
        resultMap.put("ATTR_VALUE_RANGE_DESC", StringUtils.join(rangeValues, "@@"));
        return resultMap;
    }

    public static String gzipString(String primStr)
    {
        if ((primStr == null) || (primStr.length() == 0)) {
            return primStr;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(primStr.getBytes("utf-8"));
        } catch (IOException e) {
            logger.error("压缩[" + primStr + "]时发生异常", e);
        } finally {
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    logger.error("关闭GZIPOutputStream时发生异常", e);
                }
            }
        }
        return Base64.encodeBase64String(out.toByteArray());
    }

    /**
     * 入口
     */
    public void init() {
        initAV();

        initParam();

        initParamSelect();

        initAVA();

        initRange();

        initAppVA();

        String regEx = "(\\-?[0-9]+(\\.[0-9]+)?).*";
        this.pat = Pattern.compile(regEx);
    }

    /**
     * 计算结果
     * @param partnumber
     * @param groupDatas
     * @return
     */
    protected List<String> doResult(String partnumber, List<String> groupDatas) {
        List results = new ArrayList();
        List<Map<String,String>> records = new ArrayList();
        List gcs = new ArrayList();
        List gps = new ArrayList();
        for (String v : groupDatas) {
            JSONObject jo = JSONObject.parseObject(v);
            if (jo.containsKey("SALE_CATALOG"))
                gcs.add(jo);
            else if (jo.containsKey("PRMT_CODE")) {
                gps.add(jo);
            }
        }

        Map gc;
        Map gp;
        Map<String,List> groupMap;
        Iterator i$ = gcs.iterator();
        while (i$.hasNext()) {
            gc = (Map)i$.next();
            for (i$ = gps.iterator(); i$.hasNext(); ) { gp = (Map)i$.next();
                String paramCode = (String)gp.get("PRMT_CODE");
                String saleCatalog = (String)gc.get("SALE_CATALOG");
                String aliasKey = paramCode + "_" + saleCatalog;
                List<Map<String,Object>> aliasDatas = (List)this.attrAlias.get(aliasKey);
                if (aliasDatas != null)
                    for (Map<String,Object> aliasData : aliasDatas)
                        records.add(toGether(gc, gp, aliasData));
            }
        }

        if (records.size() > 0)
        {
            groupMap = new HashMap();
            for (Map<String,String> record : records) {
                String ac = (String)record.get("ATTR_CODE");
                List datas = (List)groupMap.get(ac);
                if (datas == null) {
                    datas = new ArrayList();
                    groupMap.put(ac, datas);
                }
                datas.add(record);
            }

            for (String attrCode : groupMap.keySet()) {
                List groups = (List)groupMap.get(attrCode);
                Map result = otherJoin(partnumber, attrCode, groups);
                if (result != null) {
                    StringBuilder strBuilder = new StringBuilder();
                    strBuilder.append(((String)result.get("CMMDTY_CODE")).trim()).append(",");
                    strBuilder.append(attrCode).append(",");
                    strBuilder.append(gzipString((String)result.get("ATTR_NAME")).trim()).append(",");
                    strBuilder.append(gzipString((String)result.get("ATTR_VALUE")).trim()).append(",");
                    strBuilder.append(gzipString((String)result.get("ATTR_VALUE_RANGE_DESC")).trim());
                    results.add(strBuilder.toString().replace("\n", "").replace("\r", ""));
                }
            }
        }
        return results;
    }
}