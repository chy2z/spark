package cn.suning.hadoop.job.base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.suning.hadoop.HadoopHdfsUtil;
import cn.suning.hadoop.PropertyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class BaseReducer extends Reducer<Text, Text, NullWritable, Text>
{
    protected FileSystem fs = null;
    private boolean isChecked = false;

    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        if ((this.fs == null) && (!this.isChecked)) {
            Configuration conf = context.getConfiguration();
            String env = conf.get("env");
            PropertyUtil.setCommFlags(env);
            String hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
            this.fs = HadoopHdfsUtil.getFileSystem(hdfsBaseUrl);
            init();
            this.isChecked = true;
        }
        List<String> groupDatas = new ArrayList();
        for (Text value : values) {
            groupDatas.add(value.toString());
        }
        List<String> resultDatas = doResult(key.toString(), groupDatas);
        for (String resultData : resultDatas)
            context.write(NullWritable.get(), new Text(resultData));
    }

    public void init()
    {
    }

    protected abstract List<String> doResult(String paramString, List<String> paramList);
}
