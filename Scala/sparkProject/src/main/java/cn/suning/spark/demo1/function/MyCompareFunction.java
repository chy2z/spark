package cn.suning.spark.demo1.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cn.suning.hadoop.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

public class MyCompareFunction
        implements FlatMapFunction<Iterator<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>, String>
{
    private static final long serialVersionUID = 3040567563149147994L;
    private String delStr;
    private int delFlagIndex = -1;
    private String splitStr = "";
    private int updateTimeIndex = -1;
    private boolean flag = false;

    public MyCompareFunction(String delStr)
    {
        this.delStr = delStr;
    }

    public MyCompareFunction(String delStr, int delFlagIndex, String splitStr, int updateTimeIndex)
    {
        this.delStr = delStr;
        this.delFlagIndex = delFlagIndex;
        this.splitStr = splitStr;
        this.updateTimeIndex = updateTimeIndex;
    }

    public MyCompareFunction(String delStr, int delFlagIndex, String splitStr, int updateTimeIndex, boolean flag)
    {
        this.delStr = delStr;
        this.delFlagIndex = delFlagIndex;
        this.splitStr = splitStr;
        this.updateTimeIndex = updateTimeIndex;
        this.flag = flag;
    }

    public Iterator<String> call(Iterator<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> t)
            throws Exception {
        List list = new ArrayList();
        String tv1=null;
        String tv2=null;
        while (t.hasNext()) {
            Tuple2 v1 = (Tuple2) t.next();
            Tuple2 value = (Tuple2) v1._2;
            Iterable it1 = (Iterable) value._1;
            Iterable it2 = (Iterable) value._2;
            Iterator itr1 = it1.iterator();
            Iterator itr2 = it2.iterator();
            if ((itr1 == null) || (!itr1.hasNext())) {
                if (this.delFlagIndex != -1) {
                    if (itr2 != null) {
                        while (itr2.hasNext()) {
                            tv2 = ((String) itr2.next()).toString();
                        }
                        if (StringUtils.isNotBlank(tv2)) {
                            String[] array = tv2.split(this.splitStr);
                            int length = array.length;
                            StringBuilder item = new StringBuilder();
                            for (int i = 0; i < length; i++) {
                                if (i == this.delFlagIndex)
                                    item.append(this.delStr);
                                else if (this.updateTimeIndex == i)
                                    item.append(DateUtil.getCurrentDate("yyyy-MM-dd HH:mm:ss"));
                                else {
                                    item.append(array[i]);
                                }
                                if (i != length - 1) {
                                    item.append(this.splitStr);
                                }
                            }
                            if (this.flag) {
                                item.append(this.splitStr).append("1");
                            }
                            list.add(item.toString());
                        }
                    }
                }
            } else {
                tv1 = "";
                while (itr1.hasNext()) {
                    tv1 = ((String) itr1.next()).toString();
                }
                if ((itr2 == null) || (!itr2.hasNext())) {
                    if (this.flag) {
                        tv1 = new StringBuilder().append(tv1).append(this.splitStr).append("0").toString();
                    }
                    list.add(tv1);
                } else {
                    tv2 = "";
                    while (itr2.hasNext()) {
                        tv2 = ((String) itr2.next()).toString();
                    }
                    if (this.updateTimeIndex == -1) {
                        if (!tv1.equals(tv2)) {
                            if (this.flag) {
                                tv1 = new StringBuilder().append(tv1).append(this.splitStr).append("1").toString();
                            }
                            list.add(tv1);
                        }
                    } else if (!getNotUpdateTimeStr(tv1).equals(getNotUpdateTimeStr(tv2))) {
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

    private String getNotUpdateTimeStr(String tv) {
        StringBuilder line = new StringBuilder();
        String[] array = tv.split(this.splitStr);
        int length = array.length;
        for (int i = 0; i < length; i++) {
            if (i != this.updateTimeIndex) {
                line.append(array[i]);
            }
            if (i != length - 1) {
                line.append(this.splitStr);
            }
        }
        return line.toString();
    }
}