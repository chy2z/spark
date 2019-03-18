package cn.suning.spark.demo1.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class EmptyFilterFunction
        implements Function<Tuple2<String, String>, Boolean>
{
    private static final long serialVersionUID = 1067472609382807482L;

    public Boolean call(Tuple2<String, String> v1)
            throws Exception
    {
        return Boolean.valueOf(null != v1);
    }
}