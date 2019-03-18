package cn.suning.hadoop;

import com.google.common.base.Preconditions;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class MessageUtil
{
    private static Logger logger = Logger.getLogger(MessageUtil.class);

    public static void main(String[] args)
    {
        Preconditions.checkArgument(args.length >= 3, "runEnvFlag 没有设置。(运行环境标识：DEV代表本地开发环境、PRE代表测试环境、PRD代表生产环境)");
        PropertyUtil.setCommFlags(args[0]);
        String unitCode = args[1];
        String msg = args[2];
        sendMsg(unitCode, msg, args[0]);
    }

    public static void sendMsg(String unitCode, String msg, String devConfig)
    {
        sendMessage("ADMIN", unitCode, msg, devConfig);
    }

    public static void sendMessage(String system, String unitCode, String content, String devConfig) {
        PropertyUtil.setCommFlags(devConfig);
        String messageUrl = PropertyUtil.getProperty("messageUrl");
        try {
            String urls = "";
            system = URLEncoder.encode(system, "utf-8");
            unitCode = URLEncoder.encode(unitCode, "utf-8");
            if (StringUtils.isNotEmpty(content)) {
                content = URLEncoder.encode(content, "utf-8");
                urls = messageUrl + "?system=" + system + "&unitcode=" + unitCode + "&message=" + content;
            } else {
                urls = messageUrl + "?system=" + system + "&unitcode=" + unitCode;
            }
            URL url = new URL(urls);
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
            try {
                connection.setConnectTimeout(5000);
                connection.setReadTimeout(10000);
                connection.getResponseCode();
                connection.getResponseMessage();
                logger.info("触发短信发送链接成功" + urls);
            } catch (Exception ex) {
                logger.error("触发短信发送链接失败:" + ex.getMessage());
                throw ex;
            } finally {
                connection.disconnect();
            }
        } catch (Exception ex) {
            logger.error("触发短信发送链接失败:" + ex.getMessage());
        }
    }

    public static void sendMsg(String unitCode, String msg)
    {
        sendMessage("ADMIN", unitCode, msg);
    }

    public static void sendMessage(String system, String unitCode, String content) {
        String messageUrl = PropertyUtil.getProperty("messageUrl");
        try {
            String urls = "";
            system = URLEncoder.encode(system, "utf-8");
            unitCode = URLEncoder.encode(unitCode, "utf-8");
            if (StringUtils.isNotEmpty(content)) {
                content = URLEncoder.encode(content, "utf-8");
                urls = messageUrl + "?system=" + system + "&unitcode=" + unitCode + "&message=" + content;
            } else {
                urls = messageUrl + "?system=" + system + "&unitcode=" + unitCode;
            }
            URL url = new URL(urls);
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
            try {
                connection.setConnectTimeout(5000);
                connection.setReadTimeout(10000);
                connection.getResponseCode();
                connection.getResponseMessage();
                logger.info("触发短信发送链接成功" + urls);
            } catch (Exception ex) {
                logger.error("触发短信发送链接失败:" + ex.getMessage());
                throw ex;
            } finally {
                connection.disconnect();
            }
        } catch (Exception ex) {
            logger.error("触发短信发送链接失败:" + ex.getMessage());
        }
    }
}