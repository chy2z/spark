package cn.suning.hadoop.job.job1;

import cn.suning.hadoop.job.base.BaseMapper;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class DataMapper extends BaseMapper {
    protected Map<String, String> getGroupData(String line) {
        JSONObject sourceData = JSONObject.parseObject(line);
        String key = sourceData.getString("CMMDTY_CODE");
        sourceData.put("CMMDTY_CODE", key);
        return toMap(key, sourceData.toJSONString());
    }

    private static Map<String, String> toMap(String key, String value) {
        Map map = new HashMap();
        map.put(key, value);
        return map;
    }
}