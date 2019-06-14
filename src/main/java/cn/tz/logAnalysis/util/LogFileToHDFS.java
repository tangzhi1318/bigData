package cn.tz.logAnalysis.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @ClassName LogFileToHDFS
 * @Description 上传日志数据文件至HDFS
 * @Author Administrator
 * @Version 1.0
 **/
public class LogFileToHDFS {
       static  FileSystem fs = null;
       public static  void main (String[] args) throws Exception {
           Configuration conf = new Configuration();
           conf.set("fs.defaultFS", "hdfs://192.168.25.136:9000");
           // 获取hdfs客户端操作实例对象
           // 在windows下JAVA API远程操作hadoop HDFS文件系统时应注意用户认证权限
           fs = FileSystem.get(new URI("hdfs://192.168.25.136:9000"), conf, "root");
           fs.copyFromLocalFile(new Path("D:\\Downloads\\大数据数据源\\log_analysis2.log"), new Path("/log_analysis2.log"));
           fs.close();
       }
}
