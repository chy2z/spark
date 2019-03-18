package cn.suning.hadoop.job.base;

import java.io.IOException;
import java.util.Map;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public abstract class BaseMapper extends Mapper<LongWritable, Text, Text, Text>
{
    protected FileSystem fs = null;
    private boolean isChecked = false;

    protected void map(LongWritable lineCount, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        if ((this.fs == null) && (!this.isChecked)) {
            Configuration conf = context.getConfiguration();
            String env = conf.get("env");
            PropertyUtil.setCommFlags(env);
            String hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
            this.fs = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
            init();
            this.isChecked = true;
        }
        String line = value.toString();
        Map<String, String> datas = getGroupData(line);
        if (datas != null) {
            for (String key : datas.keySet())
                context.write(new Text(key), new Text((String) datas.get(key)));
        }
    }

    public void init()
    {
    }

    protected abstract Map<String, String> getGroupData(String paramString);
}