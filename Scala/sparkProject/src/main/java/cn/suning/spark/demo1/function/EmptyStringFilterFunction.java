package cn.suning.spark.demo1.function;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function;

public class EmptyStringFilterFunction
        implements Function<String, Boolean>
{
    private static final long serialVersionUID = -8852008484266088496L;

    public Boolean call(String v1)
            throws Exception
    {
        return Boolean.valueOf(StringUtils.isNotBlank(v1));
    }
}