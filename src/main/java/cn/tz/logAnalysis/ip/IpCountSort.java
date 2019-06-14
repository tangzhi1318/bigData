package cn.tz.logAnalysis.ip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName IpCountSort
 * @Description 根据第一次MR后的结果进行Ip出现次数排序
 * @Author Administrator
 * @Version 1.0
 **/
public class IpCountSort {
    /**
     * Text : 10.81.71.15	    13
     * IpBean : Ip实体类
     * Text : ip地址
     * @author Administrator
     */
    static class IpCountSortMapper extends Mapper <LongWritable, Text, IpBean, Text> {
        IpBean ipBean = new IpBean();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fileds = line.split("\t");
            // ip地址
            String ip = fileds[0];
            // 出现次数
            long count = Long.parseLong(fileds[1]);
            ipBean.set(ip,count);
            v.set(ip);
            //hadoop mr框架根据key自动排序(需实现排序接口)
            context.write(ipBean,v);
        }
    }
    /**
     * 输入：已排序好的IpBean,ip
     * 输出：ip，IpBean
     */
    static class IpCountSortReducer extends Reducer <IpBean, Text, Text, IpBean> {
        @Override
        protected void reduce(IpBean ipBean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //以ip为key IpBean为value
            context.write(values.iterator().next(),ipBean);
        }
    }
    /**
     * 程序主程序
     */
    public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取连接对象
        Configuration configuration = new Configuration();
        // 2.通过连接对象获取Job对象
        Job job = Job.getInstance(configuration);
        // 3.指定主程序、mapper类、reducer类
        job.setJarByClass(IpCountSort.class);
        job.setMapperClass(IpCountSortMapper.class);
        job.setReducerClass(IpCountSortReducer.class);
        // 4.设置自动分区算法
        // 5.设置Mapper程序key、value类型
        job.setMapOutputKeyClass(IpBean.class);
        job.setMapOutputValueClass(Text.class);
        // 6.设置Reducer程序key、value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IpBean.class);
        // 7.指定源数据地址(本地或HDFS)
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 8.指定MR处理后的数据存放地址(本地或HDFS)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 9.向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
        // 10.系统成功即退出
        System.exit(res?0:1);
    }
}
