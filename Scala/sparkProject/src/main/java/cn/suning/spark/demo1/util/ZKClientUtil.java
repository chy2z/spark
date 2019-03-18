package cn.suning.spark.demo1.util;

import cn.suning.hadoop.PropertyUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKClientUtil
{
    private static final Logger logger = LoggerFactory.getLogger(ZKClientUtil.class);
    private static CuratorFramework zkClient;

    public static void closeZkCient()
    {
        if (null != zkClient) {
            logger.info("关闭zkClient!");
            zkClient.close();
        }
    }

    public static CuratorFramework getZKClient()
    {
        if ((zkClient == null) || (!CuratorFrameworkState.STARTED.equals(zkClient.getState()))) {
            CuratorFramework initZkClient = null;
            ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
            initZkClient = CuratorFrameworkFactory.newClient(PropertyUtil.getProperty("zkIps"), retryPolicy);
            initZkClient.start();
            zkClient = initZkClient;
        }
        return zkClient;
    }

    public static boolean updateNode(String nodePath, String value)
    {
        boolean suc = false;
        try {
            Stat stat = (Stat)getZKClient().checkExists().forPath(nodePath);
            if (stat != null) {
                Stat opResult = (Stat)getZKClient().setData().forPath(nodePath, value.getBytes("utf-8"));
                suc = opResult != null;
            } else {
                StringBuilder stringBuilder = new StringBuilder("");
                String[] nodePaths = nodePath.split("/");
                int len = nodePaths.length;
                for (int i = 1; i < len; i++) {
                    stringBuilder.append(new StringBuilder().append("/").append(nodePaths[i]).toString());
                    String nowPath = stringBuilder.toString();
                    Stat stat_father = (Stat)getZKClient().checkExists().forPath(nowPath);
                    if (null == stat_father) {
                        getZKClient().create().forPath(nowPath);
                    }
                }
                Stat opResult = (Stat)getZKClient().setData().forPath(nodePath, value.getBytes("utf-8"));
                suc = opResult != null;
            }
        } catch (Exception e) {
            logger.error(new StringBuilder().append("更新节点出错：").append(nodePath).toString(), e);
        }
        return suc;
    }

    public static String getZkContentByPath(String path)
    {
        String result = null;
        try {
            byte[] dataBytes = (byte[])getZKClient().getData().forPath(path);
            if ((dataBytes != null) && (dataBytes.length > 0))
                result = new String(dataBytes, "utf-8");
        }
        catch (Exception e) {
            logger.error(new StringBuilder().append("显示节点出错：").append(path).toString(), e);
        }
        return result;
    }

    public static void updateNodeBySysType(String sysType, String type, String status)
    {
        String basePath = "";
        if ("dpsRe".equals(type)) {
            updateNode(new StringBuilder().append("/zkState/DPS/bigData/innerUse/re_switch_").append(sysType).toString(), status);
            logger.info(new StringBuilder().append(sysType).append(" dpsRe").append("--->").append(status).toString());
        } else if ("dpsInc".equals(type)) {
            updateNode(new StringBuilder().append("/zkState/DPS/bigData/innerUse/incr_switch").append(sysType).toString(), status);
            logger.info(new StringBuilder().append(sysType).append(" dpsInc").append("--->").append(status).toString());
        } else if ("imsRe".equals(type))
        {
            String nowVersion = DpsReVersionUtil.getNowVersion(sysType);
            String val = new StringBuilder().append("{'state':'").append(status).append("','version':'").append(nowVersion).append("'}").toString();
            updateNode(new StringBuilder().append("/zkState/DPS/").append(sysType).append("/check/full").toString(), val);
            logger.info(new StringBuilder().append(sysType).append(" imsRe").append("--->").append(val).toString());
        } else if ("imsInc".equals(type)) {
            updateNode(new StringBuilder().append("/zkState/DPS/").append(sysType).append("/check/inc").toString(), status);
            logger.info(new StringBuilder().append(sysType).append(" imsInc").append("--->").append(status).toString());
        }
    }
}
