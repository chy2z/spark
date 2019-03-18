package cn.suning.spark.demo1.function;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MyParsePairFunction
        implements PairFunction<String, String, String>
{
    private static final long serialVersionUID = 6273138651770841721L;
    private static Logger logger = Logger.getLogger(MyParsePairFunction.class);

    private String splitStr = "\t";

    private String primaryKeyIndex = "0,1";
    private Integer[] primaryIndex;

    public MyParsePairFunction(String splitStr, String primaryKeyIndex)
    {
        this.splitStr = splitStr;
        this.primaryKeyIndex = primaryKeyIndex;
        String[] primaryKeyIndexs = this.primaryKeyIndex.split(",");
        int length = primaryKeyIndexs.length;
        this.primaryIndex = new Integer[length];
        for (int i = 0; i < length; i++)
            this.primaryIndex[i] = Integer.valueOf(Integer.parseInt(primaryKeyIndexs[i]));
    }

    public Tuple2<String, String> call(String t)
            throws Exception
    {
        if (StringUtils.isEmpty(t)) {
            return null;
        }
        String[] vals = t.split(this.splitStr);
        int length = vals.length;
        String key = "";
        for (Integer index : this.primaryIndex) {
            if (index.intValue() < length) {
                key = key + vals[index.intValue()] + this.splitStr;
            } else {
                logger.info("<<<<" + t + ">>>> 数据格式有问题...");
                return null;
            }
        }
        return new Tuple2(key, t);
    }
}
