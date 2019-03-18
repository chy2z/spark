package cn.suning.spark.demo1.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

public class MyPairFlatMapFunction
        implements PairFlatMapFunction<Iterator<String>, String, String>
{
    private static final long serialVersionUID = -3095392794664318158L;
    private static Logger logger = Logger.getLogger(MyPairFlatMapFunction.class);

    private String splitStr = "\t";

    private String primaryKeyIndex = "0,1";
    private Integer[] primaryIndex;

    public MyPairFlatMapFunction(String splitStr, String primaryKeyIndex)
    {
        this.splitStr = splitStr;
        this.primaryKeyIndex = primaryKeyIndex;
        String[] primaryKeyIndexs = this.primaryKeyIndex.split(",");
        int length = primaryKeyIndexs.length;
        this.primaryIndex = new Integer[length];
        for (int i = 0; i < length; i++)
            this.primaryIndex[i] = Integer.valueOf(Integer.parseInt(primaryKeyIndexs[i]));
    }

    public Iterator<Tuple2<String, String>> call(Iterator<String> t)
            throws Exception
    {
        List list = new ArrayList();
        while (t.hasNext()) {
            String item = (String)t.next();
            String[] vals = item.split(this.splitStr);
            int length = vals.length;
            String key = "";
            boolean isFlag = true;
            for (Integer index : this.primaryIndex) {
                if (index.intValue() < length) {
                    key = key + vals[index.intValue()] + this.splitStr;
                } else {
                    isFlag = false;
                    logger.info("<<<<" + t + ">>>> 数据格式有问题...");
                }
            }
            if (isFlag) {
                list.add(new Tuple2(key, item));
            }
        }
        return list.iterator();
    }
}