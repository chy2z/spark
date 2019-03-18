package cn.suning.spark.demo1.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class DpsRowFlatMapFunction
        implements FlatMapFunction<Iterator<String>, Row>
{
    private static final long serialVersionUID = -8232172653405180283L;
    private String separator = "\t";
    private int colsLen;

    public DpsRowFlatMapFunction(String separator, int colsLen)
    {
        this.separator = separator;
        this.colsLen = colsLen;
    }

    public Iterator<Row> call(Iterator<String> t) throws Exception
    {
        List list = new ArrayList();
        while (t.hasNext()) {
            String line = (String)t.next();
            String[] vals = line.split(this.separator, -1);
            int len = vals.length;
            if (len >= this.colsLen) {
                Object[] objs = new Object[this.colsLen];
                for (int i = 0; i < this.colsLen; i++) {
                    objs[i] = vals[i];
                }
                list.add(RowFactory.create(objs));
            }
        }
        return list.iterator();
    }
}