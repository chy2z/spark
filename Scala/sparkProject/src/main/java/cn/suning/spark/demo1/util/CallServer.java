package cn.suning.spark.demo1.util;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;



public class CallServer extends Thread
{
    private static Logger LOG = Logger.getLogger(CallServer.class);
    public String url;

    public void run()
    {
        callServer(this.url);
    }

    private String callServer(String urls)
    {
        try
        {
            URL url = new URL(urls);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.addRequestProperty("User-Agent", "admin");
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(300);
            InputStream is = conn.getInputStream();
            byte i = 0;
            List byteList = new ArrayList();
            while ((i = (byte)is.read()) != -1) {
                byteList.add(Byte.valueOf(i));
            }
            byte[] bytes = new byte[byteList.size()];
            for (int j = 0; j < bytes.length; j++) {
                bytes[j] = ((Byte)byteList.get(j)).byteValue();
            }
            String returnVal = new String(bytes);
            if ("exception".equalsIgnoreCase(returnVal)) {
                return "remoteException";
            }
            return returnVal.toLowerCase();
        }
        catch (Exception e) {
            LOG.info("请求URL过程出现异常：" + urls + "：" + e.toString(), e);
        }return "localException";
    }
}