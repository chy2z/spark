package cn.suning.spark.demo1.util;

import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;

public class RryoRegistrator implements KryoRegistrator
{
    public void registerClasses(Kryo kryo)
    {
        /*
        kryo.register(ExtFieldCommPojo.class);
        kryo.register(NewProductPojo.class);
        kryo.register(VendorSaleCatalogPojo.class);
        kryo.register(MeFlagshipOperationPojo.class);
        kryo.register(MeFlagshipOperCataPojo.class);
        kryo.register(ProductBPojo.class);
        kryo.register(ProductBHBasePojo.class);
        kryo.register(TicketBasicPojo.class);
        kryo.register(GoodsParametersPojo.class);
        kryo.register(Xcatengrp.class);
        kryo.register(FixedBundle.class);
        kryo.register(EbookEditorcategPojo.class);
        kryo.register(ActivityCouponPojo.class);
        kryo.register(CatgroupsPojo.class);
        */
        kryo.register(ProductPojo.class);
    }
}