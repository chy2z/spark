package cn.suning.spark.test.hdfs;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * 在自定义类中实现KryoRegistrator接口的registerClasses方法
 */
public class MyRegistrator implements KryoRegistrator
{
    public void registerClasses(Kryo kryo)
    {
        kryo.register(PersonPojo.class);
    }
}