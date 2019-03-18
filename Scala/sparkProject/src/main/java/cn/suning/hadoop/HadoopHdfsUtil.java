package cn.suning.hadoop;

import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

public class HadoopHdfsUtil
{
    private static Logger logger = Logger.getLogger(HadoopHdfsUtil.class);
    private static final String HADOOP_USER_NAME = "sousuo";
    private static final Configuration conf = new Configuration();

    /**
     * 获取hdfs文件系统
     * @param rootPath
     * @return
     */
    public static FileSystem getFileSystem(String rootPath)
    {
        FileSystem tmpFileSystem = null;
        try {
            //设置用户
            System.setProperty("HADOOP_USER_NAME", "sousuo");
            tmpFileSystem = FileSystem.newInstance(new URI(rootPath), conf);
        } catch (Exception e) {
            logger.error("fileSystem 初始出错：", e);
        }
        return tmpFileSystem;
    }

    /**
     * 删除目录和文件
     * 备注: 默认递归删除
     * @param fileSystem
     * @param path
     */
    public static void deleteHdfsDir(FileSystem fileSystem, String path) {
        try {
            deleteHdfsDir(fileSystem, path, true);
        } catch (Exception e) {
            logger.error("fileSystem 关闭出错：", e);
        }
    }

    /**
     * 删除目录和文件
     * @param fileSystem
     * @param path
     * @param r 是否递归删除目录和文件
     */
    public static void deleteHdfsDir(FileSystem fileSystem, String path,boolean r) {
        try {
            fileSystem.delete(new Path(path), r);
        } catch (Exception e) {
            logger.error("fileSystem 关闭出错：", e);
        }
    }

    /**
     * 关闭hdfs文件系统
     * @param fileSystem
     */
    public static void closeFileSystem(FileSystem fileSystem)
    {
        try
        {
            if (fileSystem != null) {
                fileSystem.close();
                fileSystem = null;
            }
        } catch (Exception e) {
            logger.error("fileSystem 关闭出错：", e);
        }
    }

    /**
     * 获取指定目录的第一级文件目录和文件列表
     * @param fileSystem
     * @param path
     * @return
     */
    public static FileStatus[] getFilesStatus(FileSystem fileSystem, Path path)
    {
        FileStatus[] filestatus = null;
        try {
            filestatus = fileSystem.listStatus(path);
        } catch (IOException e) {
            logger.error("获取文件列表 出错：", e);
        }
        return filestatus;
    }

    /**
     * 获取指定目录的第一级文件目录和文件列表
     * @param fileSystem
     * @param pathStr
     * @return
     */
    public static FileStatus[] getFilesStatus(FileSystem fileSystem, String pathStr) {
        FileStatus[] filestatus = null;
        filestatus = getFilesStatus(fileSystem, new Path(pathStr));
        return filestatus;
    }

    /**
     * 获取文件状态
     * @param fileSystem
     * @param pathStr
     * @return
     */
    public static FileStatus getFileStatus(FileSystem fileSystem, String pathStr) {
        FileStatus filestatus = null;
        try {
            filestatus = fileSystem.getFileStatus(new Path(pathStr));
            return filestatus;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return filestatus;
    }

    /**
     * 目录存储的位置
     * @param fileSystem
     * @param pathStr
     * @return
     */
    public static BlockLocation[] getFileBlockLocations(FileSystem fileSystem, String pathStr) {
        BlockLocation[] blkLocation = null;
        FileStatus fileStatus = getFileStatus(fileSystem, pathStr);
        if (fileStatus != null) {
            try {
                blkLocation = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return blkLocation;
    }

    public static int checkDataPath(FileSystem fileSystem, String pathStr)
    {
        int result = 1;
        try {
            if (!pathStr.endsWith("/")) {
                pathStr = pathStr + "/";
            }
            boolean exists = fileSystem.exists(new Path(pathStr));
            boolean existsFlag = fileSystem.exists(new Path(pathStr + "_SUCCESS"));
            boolean tempExists = fileSystem.exists(new Path(pathStr + "/_temporary"));
            if ((exists) && (existsFlag) && (!tempExists))
            {
                result = 0;
            }
        } catch (Exception e) {
            logger.error("路径：" + pathStr + " 出错", e);
        }

        logger.info(result + "<-->" + pathStr);
        return result;
    }

    public static int checkDataPathNoSuc(FileSystem fileSystem, String pathStr)
    {
        int result = 1;
        try {
            if (!pathStr.endsWith("/")) {
                pathStr = pathStr + "/";
            }
            boolean exists = fileSystem.exists(new Path(pathStr));
            if (exists)
            {
                result = 0;
            }
        } catch (Exception e) {
            logger.error("路径：" + pathStr + " 出错", e);
        }
        logger.info(result + "<-->" + pathStr);
        return result;
    }

    public static boolean checkHdfsSuccess(FileSystem fileSystem, String pathStr)
    {
        boolean flag = false;
        try {
            if (!pathStr.endsWith("/")) {
                pathStr = pathStr + "/";
            }
            boolean exists = fileSystem.exists(new Path(pathStr));
            boolean existsFlag = fileSystem.exists(new Path(pathStr + "_SUCCESS"));
            boolean tempExists = fileSystem.exists(new Path(pathStr + "/_temporary"));
            if ((exists) && (existsFlag) && (!tempExists))
            {
                flag = true;
            }
        } catch (Exception e) {
            logger.error("路径：" + pathStr + " 出错", e);
        }
        logger.info(flag + "<-->" + pathStr);
        return flag;
    }

    public static String[] getLastNextDataPath(FileSystem fileSystem, String pathStr)
    {
        String[] incPath = { "", "" };
        String lastDataPath = "";
        FileStatus[] fileStatus = null;
        try {
            boolean exists = fileSystem.exists(new Path(pathStr));
            if (exists) {
                List list = new ArrayList();
                fileStatus = fileSystem.listStatus(new Path(pathStr));
                if (fileStatus.length >= 2) {
                    for (FileStatus file : fileStatus) {
                        if (file.isDirectory()) {
                            list.add(file.getPath().getName());
                        }
                    }
                    if (list.size() > 0) {
                        lastDataPath = (String)Collections.max(list);
                        incPath[0] = lastDataPath;
                        list.remove(lastDataPath);
                        lastDataPath = (String)Collections.max(list);
                        incPath[1] = lastDataPath;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("获取最后数据更新的的路径：", e);
        }
        return incPath;
    }

    public static String getLastSucTime(FileSystem fileSystem, String pathStr)
    {
        if (!pathStr.endsWith("/")) {
            pathStr = pathStr + File.separator;
        }
        String lastDataPath = "";
        FileStatus[] fileStatus = null;
        try {
            boolean exists = fileSystem.exists(new Path(pathStr));
            if (exists) {
                List list = new ArrayList();
                fileStatus = fileSystem.listStatus(new Path(pathStr));
                for (FileStatus file : fileStatus) {
                    if (file.isDirectory()) {
                        boolean sucExists = fileSystem.exists(new Path(pathStr + file.getPath().getName() + "/_SUCCESS"));
                        if (sucExists) {
                            list.add(file.getPath().getName());
                        }
                    }
                }
                if (list.size() > 0)
                    lastDataPath = (String)Collections.max(list);
            }
        }
        catch (Exception e) {
            logger.error("获取最后_SUCCESS的路径：", e);
        }
        return lastDataPath;
    }

    public static String getLastSucTimePath(FileSystem fileSystem, String pathStr)
    {
        if (!pathStr.endsWith("/")) {
            pathStr = pathStr + File.separator;
        }
        String lastDataPath = "";
        FileStatus[] fileStatus = null;
        try {
            boolean exists = fileSystem.exists(new Path(pathStr));
            if (exists) {
                List list = new ArrayList();
                fileStatus = fileSystem.listStatus(new Path(pathStr));
                for (FileStatus file : fileStatus) {
                    if (file.isDirectory()) {
                        boolean sucExists = fileSystem.exists(new Path(pathStr + file.getPath().getName() + "/_SUCCESS"));
                        boolean tempExists = fileSystem.exists(new Path(pathStr + file.getPath().getName() + "/_temporary"));
                        if ((sucExists) && (!tempExists)) {
                            list.add(file.getPath().getName());
                        }
                    }
                }
                if (list.size() > 0)
                    lastDataPath = (String)Collections.max(list);
            }
        }
        catch (Exception e) {
            logger.error("获取最后_SUCCESS的路径：", e);
        }
        return lastDataPath;
    }

    public static String getSuccessAllPath(FileSystem fileSystem, String outPath, String hdfsBaseUrl, String currentDateStr)
    {
        String tempPath = inputDataPathChange(hdfsBaseUrl + outPath, currentDateStr);
        String lastTimeStrOut = getLastSucTimePath(fileSystem, tempPath);
        return tempPath + lastTimeStrOut;
    }

    public static List<String> getSucTimePath(FileSystem fileSystem, String pathStr)
    {
        if (!pathStr.endsWith("/")) {
            pathStr = pathStr + File.separator;
        }
        FileStatus[] fileStatus = null;
        List list = new ArrayList();
        try {
            boolean exists = fileSystem.exists(new Path(pathStr));
            if (exists) {
                fileStatus = fileSystem.listStatus(new Path(pathStr));
                for (FileStatus file : fileStatus) {
                    if (file.isDirectory()) {
                        boolean sucExists = fileSystem.exists(new Path(pathStr + file.getPath().getName() + "/_SUCCESS"));
                        boolean tempExists = fileSystem.exists(new Path(pathStr + file.getPath().getName() + "/_temporary"));
                        if ((sucExists) && (!tempExists)) {
                            list.add(file.getPath().getName());
                        }
                    }
                }
            }
            Collections.sort(list, new Comparator<String>() {
                public int compare(String o1, String o2) {
                    if (o1.compareTo(o2) >= 0) {
                        return 1;
                    }
                    return 0;
                }
            });
        }
        catch (Exception e) {
            logger.error("获取最后_SUCCESS的路径：", e);
        }
        return list;
    }


    /**
     * 检查路径是否存在
     * @param fileSystem
     * @param hdfsPath
     * @return
     */
    public static boolean checkSucPath(FileSystem fileSystem, String hdfsPath)
    {
        boolean sucExists = false;
        try {
            sucExists = fileSystem.exists(new Path(new StringBuilder().append(hdfsPath).append("/_SUCCESS").toString()));
        } catch (Exception e) {
            logger.info("检查 _SUCCESS", e);
        }
        return sucExists;
    }

    /**
     * 清理路径
     * @param path
     * @param fileSystem
     * @param saveNum
     */
    public static void clearHdfs(String path, FileSystem fileSystem, int saveNum) {
        path = path + DateUtil.getTime(0);
        List list = new ArrayList();
        try {
            if (!path.endsWith("/")) {
                path = path + "/";
            }
            FileStatus[] fileStatus = fileSystem.listStatus(new Path(path));
            for (FileStatus file : fileStatus)
                if (checkSucPath(fileSystem, path + File.separator + file.getPath().getName()))
                    list.add(file.getPath().getName());
        } catch (Exception e) {
            logger.error("获取最后数据更新的的路径：", e);
        }
        Collections.sort(list);
        int length = list.size();
        if (saveNum >= length) {
            return;
        }
        for (int i = 0; i < length - saveNum; i++) {
            deleteHdfsDir(fileSystem, path + File.separator + (String) list.get(i));
        }
    }


    /**
     * 输入路径改变
     * @param inputPath
     * @param suffixPath
     * @return
     */
    public static String inputDataPathChange(String inputPath, String suffixPath)
    {
        String pathString = "";
        if (!inputPath.endsWith("/")) {
            inputPath = inputPath + "/";
        }
        pathString = inputPath + suffixPath;
        if (!pathString.endsWith("/")) {
            pathString = pathString + "/";
        }
        return pathString;
    }

    /**
     * 输入路径改变
     * @param fileSystem
     * @param inputPath
     * @param dayStr
     * @return
     */
    public static String inputDataPathChange(FileSystem fileSystem, String inputPath, String dayStr)
    {
        String pathString = "";
        if (!inputPath.endsWith("/")) {
            inputPath = inputPath + "/";
        }
        pathString = inputPath + dayStr + "/";
        if (StringUtils.isNotBlank(pathString)) {
            String lastTimeStr = getLastSucTimePath(fileSystem, pathString);
            pathString = pathString + lastTimeStr;
        }
        if (!pathString.endsWith("/")) {
            pathString = pathString + "/";
        }
        return pathString;
    }

    /**
     * 输出路径改变
     * @param outputPath
     * @param suffixPath
     * @return
     */
    public static String outputDataPathChange(String outputPath, String suffixPath) {
        String pathString = "";
        if (!outputPath.endsWith("/")) {
            outputPath = outputPath + "/";
        }
        pathString = outputPath + suffixPath;
        if (!pathString.endsWith("/")) {
            pathString = pathString + "/";
        }
        return pathString;
    }

    /**
     * 检查文件是否存在
     * @param fileSystem
     * @param pathStr
     * @return
     * @throws IOException
     */
    public static boolean checkFileExists(FileSystem fileSystem,  String pathStr) throws IOException {
        return fileSystem.exists(new Path(pathStr));
    }

    /**
     * 创建目录
     * @param fileSystem
     * @param pathStr
     * @return
     */
    public static boolean mkHdfsDir(FileSystem fileSystem,String pathStr) {
        try {
            Path dfs = new Path(pathStr);
            return fileSystem.mkdirs(dfs);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 重命名hdfs目录
     *
     * 备注：文件夹要有权限，修改别的用户创建的文件夹要有drwxrwxrwx权限
     * @param fileSystem
     * @param srcPath  hdfs://master:9000/xxxx/xxxx/重命明目录旧
     * @param destPath hdfs://master:9000/xxxx/xxxx/重命明目录新
     */
    public static void renameHdfsDir(FileSystem fileSystem, String srcPath, String destPath)
    {
        try
        {
            fileSystem.rename(new Path(srcPath), new Path(destPath));
        } catch (Exception e) {
            logger.error("出错 移动：" + srcPath + " 到：" + destPath, e);
        }
    }

    /**
     * hdfs之间拷贝
     * 备注:拷贝目录的所有内容
     * @param srcPath hdfs://master:9000/xxxx/xxxx/test.txt
     * @param dstPath hdfs://master:9000/xxxx/xxxx/temp.txt
     * @return
     * @throws IOException
     */
    public static boolean copyFromHdfs(String srcPath, String dstPath) throws IOException {
        Path src = new Path(srcPath);
        Path dst = new Path(dstPath);
        try {
            return FileUtil.copy(src.getFileSystem(conf), src, dst.getFileSystem(conf), dst, false, conf);
        } catch (IOException e) {
            throw e;
        }
    }

    /**
     * 从本地拷贝文件到hdfs
     * @param fileSystem
     * @param source  /usr/local/filecontent/demo.txt
     * @param destination hdfs://master:9000/xxxx/xxxx/test.txt
     * @throws IOException
     */
    public static void copyFromLocal(FileSystem fileSystem,String source,String destination) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(source));
        //HDFS读写的配置文件
        Configuration conf = new Configuration();
        //调用Filesystem的create方法返回的是FSDataOutputStream对象
        //该对象不允许在文件中定位，因为HDFS只允许一个已打开的文件顺序写入或追加
        //FileSystem fs = FileSystem.get(URI.create(destination),conf);
        OutputStream out = fileSystem.create(new Path(destination));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * 写入sequenceFile文件
     * @param fileSystem
     * @param dstPath
     * @param map
     */
    public static void sequenceFileWriter(FileSystem fileSystem,String dstPath,Map<String,String> map) {
        SequenceFile.Writer writer = null;
        try {
            Path path = new Path(dstPath);
            Text key = new Text();
            Text value = new Text();
            writer = SequenceFile.createWriter(fileSystem, conf, path, key.getClass(), value.getClass());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                key.set(entry.getKey());
                value.set(entry.getValue());
                writer.append(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    /**
     * 读取sequenceFile文件
     * @param fileSystem
     * @param dstPath
     */
    public static Map<String,String> sequenceFileReader(FileSystem fileSystem,String dstPath) {
        SequenceFile.Reader reader = null;
        Map<String, String> map = new HashMap<>();
        try {
            Path path = new Path(dstPath);
            reader = new SequenceFile.Reader(fileSystem, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                map.put(key.toString(), value.toString());
                position = reader.getPosition();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
        return map;
    }

    public static void main(String[] args) {
        //Preconditions.checkArgument(args.length >= 1, "runEnvFlag 没有设置。(运行环境标识：DEV代表本地开发环境、PRE代表测试环境、PRD代表生产环境) service：表示业务单元");
        PropertyUtil.setCommFlags("dev,");
        String hdfsBaseUrl = PropertyUtil.getProperty("hdfsBaseUrl");
        FileSystem fileSystem = getFileSystem(hdfsBaseUrl);


        try {
            String destination= PropertyUtil.getProperty("hdfsBaseUrl") + "/rdd/rdd2dataset.txt";
            copyFromLocal(fileSystem, "/opt/hadoop/rdd2dataset.txt", destination);
        } catch (IOException e) {
            e.printStackTrace();
        }




        /*
        try {
            String srcPath= PropertyUtil.getProperty("hdfsBaseUrl") + "/hive";
            String dstPath=PropertyUtil.getProperty("hdfsBaseUrl") + "/add";
            copyFromHdfs(srcPath,dstPath);
        }  catch (IOException e) {
            e.printStackTrace();
        }
        */

        /*
        try {
            String srcDir=PropertyUtil.getProperty("hdfsBaseUrl") + "/folders/temp";
            String desDir=PropertyUtil.getProperty("hdfsBaseUrl") + "/folders/temp1";
            renameHdfsDir(fileSystem,srcDir,desDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
        */

        /*
        mkHdfsDir(fileSystem,PropertyUtil.getProperty("hdfsBaseUrl") + "/logs/result_log");
        deleteHdfsDir(fileSystem,PropertyUtil.getProperty("hdfsBaseUrl") + "/logs/result_log");
        */

        /*
        try {
            String srcDir1 = PropertyUtil.getProperty("hdfsBaseUrl") + "/add/test.txt";
            String srcDir2 = PropertyUtil.getProperty("hdfsBaseUrl") + "/add/sql.txt";
            System.out.println(checkFileExists(fileSystem, srcDir1) ? 1 : 0);
            System.out.println(checkFileExists(fileSystem, srcDir2) ? 1 : 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        */

        /*
        String desDir = PropertyUtil.getProperty("hdfsBaseUrl")+"/sql/sql-1/sql-2";
        mkHdfsDir(fileSystem,desDir);
        String desDir2 = PropertyUtil.getProperty("hdfsBaseUrl")+"/sql";
        FileStatus[] fileStatuses = getFilesStatus(fileSystem, desDir2);
        for (int i = 0; i < fileStatuses.length; i++) {
            System.out.println(fileStatuses[i].getPath().toString());
        }
        */

        /*
        String srcDir = PropertyUtil.getProperty("hdfsBaseUrl") + "/add/sql.txt";
        FileStatus fileStatus = getFileStatus(fileSystem, srcDir);
        System.out.println(fileStatus.getPath().toString());
        */

        /*
        try {
            String srcDir = PropertyUtil.getProperty("hdfsBaseUrl") + "/copy/copy.txt/part-00000";
            BlockLocation[] blockLocations = getFileBlockLocations(fileSystem, srcDir);
            for (int i = 0; i < blockLocations.length; i++) {
                String[] hosts = blockLocations[i].getHosts();
                System.out.println("block_" + i + "_Location:" + hosts[0]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        */

        /*
        String desDir = PropertyUtil.getProperty("hdfsBaseUrl") + "/sequence/file";
        Map<String, String> map = new HashMap<>();
        map.put("苏宁", "1");
        map.put("ali", "2");
        map.put("京东", "3");
        sequenceFileWriter(fileSystem, desDir, map);
        map.clear();
        map = sequenceFileReader(fileSystem, desDir);
        for (Map.Entry<String, String> entry : map.entrySet()) {
           System.out.println(entry.getKey()+"--->"+entry.getValue());
        }
        */


        closeFileSystem(fileSystem);
    }

}