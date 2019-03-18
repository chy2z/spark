package cn.suning.spark.demo1.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cn.suning.spark.demo1.util.CommUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class DpsRowByIndexsFlatMapFunction
        implements FlatMapFunction<Iterator<String>, Row>
{
    private static final long serialVersionUID = 5466945001143062036L;
    private String separator = "\t";
    private int[] indexs;
    private String partnumberIndexs;
    private String vendorIndexs;
    private String zipIndexs;
    private int colLen;

    public DpsRowByIndexsFlatMapFunction(String separator, String indexStr)
    {
        this.separator = separator;
        String[] indexStrs = indexStr.split(",", -1);
        this.colLen = indexStrs.length;
        int[] tmp = new int[this.colLen];
        for (int i = 0; i < this.colLen; i++) {
            tmp[i] = Integer.parseInt(indexStrs[i]);
        }
        this.indexs = tmp;
    }

    public DpsRowByIndexsFlatMapFunction(String separator, String indexStr, String partnumberIndexs, String vendorIndexs, String zipIndexs)
    {
        this.separator = separator;
        String[] indexStrs = indexStr.split(",", -1);
        this.colLen = indexStrs.length;
        int[] tmp = new int[this.colLen];
        for (int i = 0; i < this.colLen; i++) {
            tmp[i] = Integer.parseInt(indexStrs[i]);
        }
        this.indexs = tmp;
        this.partnumberIndexs = partnumberIndexs;
        this.vendorIndexs = vendorIndexs;
        this.zipIndexs = zipIndexs;
    }

    public Iterator<Row> call(Iterator<String> t)
            throws Exception
    {
        List list = new ArrayList();
        while (t.hasNext()) {
            String line = (String)t.next();
            if (line != null)
            {
                String[] vals = line.split(this.separator, -1);

                if ((vals != null) && (vals.length > 1))
                {
                    Object[] objs = new Object[this.colLen];
                    for (int i = 0; i < this.colLen; i++) {
                        String v = vals[this.indexs[i]];
                        if ((StringUtils.isNotEmpty(this.partnumberIndexs)) && (this.partnumberIndexs.indexOf("," + i + ",") >= 0))
                            v = CommUtil.getNinePartnumber(v);
                        else if ((StringUtils.isNotEmpty(this.vendorIndexs)) && (this.vendorIndexs.indexOf("," + i + ",") >= 0))
                            v = CommUtil.getVendr(v);
                        else if ((StringUtils.isNotEmpty(this.zipIndexs)) && (this.zipIndexs.indexOf("," + i + ",") >= 0)) {
                            v = CommUtil.unGzipString(v);
                        }
                        objs[i] = v;
                    }
                    list.add(RowFactory.create(objs));
                }
            }
        }
        return list.iterator();
    }
}