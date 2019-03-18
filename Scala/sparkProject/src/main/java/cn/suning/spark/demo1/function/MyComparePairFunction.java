package cn.suning.spark.demo1.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.suning.hadoop.DateUtil;
import cn.suning.spark.demo1.util.CommUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

public class MyComparePairFunction
        implements FlatMapFunction<Iterator<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>, String>
{
    private static final long serialVersionUID = -1889125761088579323L;
    private String delStr;
    private int delFlagIndex = -1;
    private String splitStr = "";
    private String updateTimeIndex = "-1";

    private boolean flag = false;
    private boolean pciFlag = false;
    Map<Integer, String> exceptMap = new HashMap();

    public MyComparePairFunction(String delStr)
    {
        this.delStr = delStr;
    }

    public MyComparePairFunction(String delStr, int delFlagIndex, String splitStr, String updateTimeIndex)
    {
        this.delStr = delStr;
        this.delFlagIndex = delFlagIndex;
        this.splitStr = splitStr;
        this.updateTimeIndex = updateTimeIndex;
    }

    public MyComparePairFunction(String delStr, int delFlagIndex, String splitStr, String updateTimeIndex, boolean flag)
    {
        this.delStr = delStr;
        this.delFlagIndex = delFlagIndex;
        this.splitStr = splitStr;
        this.updateTimeIndex = updateTimeIndex;
        this.flag = flag;
    }

    public MyComparePairFunction(String delStr, int delFlagIndex, String splitStr, String updateTimeIndex, Map<Integer, String> exceptMap)
    {
        this.delStr = delStr;
        this.delFlagIndex = delFlagIndex;
        this.splitStr = splitStr;
        this.updateTimeIndex = updateTimeIndex;
        this.exceptMap = exceptMap;
    }

    public MyComparePairFunction(String delStr, int delFlagIndex, String splitStr, String updateTimeIndex, Map<Integer, String> exceptMap, boolean pciFlag)
    {
        this.delStr = delStr;
        this.delFlagIndex = delFlagIndex;
        this.splitStr = splitStr;
        this.updateTimeIndex = updateTimeIndex;
        this.exceptMap = exceptMap;
        this.pciFlag = pciFlag;
    }

    public Iterator<String> call(Iterator<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> t)
            throws Exception
    {
        List list = new ArrayList();
        while (t.hasNext()) {
            Tuple2 v1 = (Tuple2)t.next();
            Tuple2 value = (Tuple2)v1._2;
            Iterable newIt = (Iterable)value._1;
            Iterable oldIt = (Iterable)value._2;
            Iterator newIter = newIt.iterator();
            Iterator oldIter = oldIt.iterator();
            boolean isDelFlag = (newIter == null) || (!newIter.hasNext());
            if (isDelFlag) {
                list.add(getDelItemStr(oldIter));
            }
            else {
                String tv1 = "";
                while (newIter.hasNext()) {
                    tv1 = (String)newIter.next();
                }
                if ((oldIter == null) || (!oldIter.hasNext())) {
                    if (this.pciFlag)
                        tv1 = getAddTimeStr(tv1);
                    else {
                        tv1 = getUpdateTimeStr(tv1);
                    }
                    if (this.flag) {
                        tv1 = new StringBuilder().append(tv1).append(this.splitStr).append("0").toString();
                    }
                    list.add(tv1);
                }
                else {
                    String tv2 = "";
                    while (oldIter.hasNext()) {
                        tv2 = (String)oldIter.next();
                    }
                    Map oldMapData = new HashMap();

                    if (this.pciFlag)
                        tv2 = getUpdateTimeStr(oldMapData, tv2, 23, 33, 36);
                    else {
                        tv2 = getUpdateTimeStr(tv2);
                    }

                    if (this.pciFlag)
                        tv1 = parseUpdateTimeStr(oldMapData, tv1, 23, 33, 36);
                    else {
                        tv1 = getUpdateTimeStr(tv1);
                    }

                    if ("-1".equals(this.updateTimeIndex)) {
                        if (!tv1.equals(tv2)) {
                            if (this.flag) {
                                tv1 = new StringBuilder().append(tv1).append(this.splitStr).append("1").toString();
                            }
                            list.add(tv1);
                        }

                    }
                    else if (!getNotUpdateTimeStr(tv1).equals(getNotUpdateTimeStr(tv2))) {
                        if (this.flag) {
                            tv1 = new StringBuilder().append(tv1).append(this.splitStr).append("1").toString();
                        }
                        list.add(tv1);
                    }
                }
            }
        }

        return list.iterator();
    }

    private String getDelItemStr(Iterator<String> oldIter)
    {
        if (this.delFlagIndex == -1)
            return "";
        if (oldIter != null) {
            String tv2 = "";
            while (oldIter.hasNext()) {
                tv2 = (String)oldIter.next();
            }
            if (StringUtils.isNotBlank(tv2)) {
                String[] array = tv2.split(this.splitStr);
                int length = array.length;
                StringBuilder item = new StringBuilder();
                for (int i = 0; i < length; i++) {
                    if (i == this.delFlagIndex)
                        item.append(this.delStr);
                    else if (this.updateTimeIndex.startsWith(new StringBuilder().append("@").append(i).append("@").toString()))
                        item.append(DateUtil.curTimeAdd(3));
                    else {
                        item.append(array[i]);
                    }
                    if (i != length - 1) {
                        item.append(this.splitStr);
                    }
                }
                if (this.flag) {
                    item.append(this.splitStr).append("2");
                }
                return item.toString();
            }
        }
        return "";
    }

    private String getNotUpdateTimeStr(String tv) {
        StringBuilder line = new StringBuilder();
        String[] array = tv.split(this.splitStr);
        int length = array.length;
        for (int i = 0; i < length; i++) {
            if (!this.updateTimeIndex.contains(new StringBuilder().append("@").append(i).append("@").toString())) {
                String ke = (String)this.exceptMap.get(Integer.valueOf(i));
                if ((StringUtils.isNotBlank(ke)) && (CommUtil.isNotBlankAndNull(array[i]))) {
                    Map map = parseJsonStr(array[i]);
                    String[] arr = ke.split("#");
                    for (String ar : arr) {
                        map.remove(ar);
                    }
                    line.append(map.toString());
                } else {
                    line.append(array[i]);
                }
            }
            if (i != length - 1) {
                line.append(this.splitStr);
            }
        }
        return line.toString();
    }

    private String parseUpdateTimeStr(Map<String, String> oldMapData, String tv, int inventoryIndex, int priceIndex, int cityIndex)
    {
        String newPrice = "";
        String newInventory = "";
        String newCityIds = "";
        StringBuilder line = new StringBuilder();
        String[] array = tv.split(this.splitStr);
        int length = array.length;
        for (int i = 0; i < length; i++) {
            if (i == 1) {
                line.append(DateUtil.curTimeAdd(3));
            } else if (i == inventoryIndex) {
                newInventory = array[i];
                line.append(array[i]);
            } else if (i == priceIndex) {
                newPrice = array[i];
                line.append(array[i]);
            } else if (i == cityIndex) {
                newCityIds = array[i];
                line.append(array[i]);
            } else if (i == 86) {
                String oldPrice = (String)oldMapData.get("price");
                String oldInventory = (String)oldMapData.get("inventory");
                String oldCityId = (String)oldMapData.get("city");
                String delFlag = (String)oldMapData.get("delFlag");
                StringBuilder sb = new StringBuilder();
                if ((oldPrice != null) && (oldPrice.equals(newInventory))) {
                    sb.append("p");
                }
                if ((oldCityId != null) && (oldCityId.equals(newCityIds))) {
                    sb.append("c");
                }
                if ((oldInventory != null) && (oldInventory.equals(newPrice))) {
                    sb.append("i");
                }
                if ("1".equals(delFlag)) {
                    sb.append("del");
                }
                line.append(sb.toString());
            } else {
                line.append(array[i]);
            }
            if (i != length - 1) {
                line.append(this.splitStr);
            }
        }
        return line.toString();
    }

    private String getUpdateTimeStr(Map<String, String> newMapData, String tv, int inventoryIndex, int priceIndex, int cityIndex)
    {
        String newPrice = "";
        String newInventory = "";
        String newCityIds = "";
        StringBuilder line = new StringBuilder();
        String[] array = tv.split(this.splitStr);
        int length = array.length;
        String delFlag = "0";
        for (int i = 0; i < length; i++) {
            if (i == 1) {
                line.append(DateUtil.curTimeAdd(3));
            } else if (i == inventoryIndex) {
                newInventory = array[i];
                line.append(array[i]);
            } else if (i == priceIndex) {
                newPrice = array[i];
                line.append(array[i]);
            } else if (i == cityIndex) {
                newCityIds = array[i];
                line.append(array[i]);
            } else if (i == 86) {
                line.append("pci");
            } else {
                line.append(array[i]);
            }
            if (i != length - 1) {
                line.append(this.splitStr);
            }
        }
        if (length > 4) {
            delFlag = array[2];
        }
        newMapData.put("price", newPrice);
        newMapData.put("inventory", newInventory);
        newMapData.put("city", newCityIds);
        newMapData.put("delFlag", delFlag);
        return line.toString();
    }

    private String getAddTimeStr(String tv)
    {
        StringBuilder line = new StringBuilder();
        String[] array = tv.split(this.splitStr);
        int length = array.length;
        for (int i = 0; i < length; i++) {
            if (i == 1)
                line.append(DateUtil.curTimeAdd(3));
            else if (i == 86)
                line.append("add");
            else {
                line.append(array[i]);
            }
            if (i != length - 1) {
                line.append(this.splitStr);
            }
        }
        return line.toString();
    }

    private String getUpdateTimeStr(String tv)
    {
        StringBuilder line = new StringBuilder();
        String[] array = tv.split(this.splitStr);
        int length = array.length;
        for (int i = 0; i < length; i++) {
            if (i == 1)
                line.append(DateUtil.curTimeAdd(3));
            else {
                line.append(array[i]);
            }
            if (i != length - 1) {
                line.append(this.splitStr);
            }
        }
        return line.toString();
    }

    private Map<String, Object> parseJsonStr(String v)
    {
        Map obj = null;
        try {
            obj = CommUtil.parseJsonString(v);
        } catch (Exception e) {
        }
        if (obj == null) {
            obj = new HashMap();
        }
        return obj;
    }
}