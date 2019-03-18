package cn.suning.spark.demo1.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

class PairFlatMapFunctionTmp implements PairFlatMapFunction<Iterator<String>, String, Integer>
{
    private static final long serialVersionUID = -7090435111813903121L;
    private String separator;
    private int[] keyIntIndex;
    private int keyLen;

    public PairFlatMapFunctionTmp(String separator, int[] keyIntIndex, int keyLen)
    {
        this.separator = separator;
        this.keyIntIndex = keyIntIndex;
        this.keyLen = keyLen;
    }

    public Iterator<Tuple2<String, Integer>> call(Iterator<String> t) throws Exception
    {
        List list = new ArrayList();
        while (t.hasNext()) {
            String[] vals = new StringBuilder().append((String)t.next()).append("").toString().split(this.separator, -1);
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < this.keyLen; i++) {
                stringBuilder.append(new StringBuilder().append(vals[this.keyIntIndex[i]]).append("@").toString());
            }
            list.add(new Tuple2(stringBuilder.toString(), Integer.valueOf(1)));
        }
        return list.iterator();
    }
}