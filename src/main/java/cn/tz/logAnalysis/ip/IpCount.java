package cn.tz.logAnalysis.ip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ClassName IpCount
 * @Description Ip统计
 * @Author Administrator
 * @Version 1.0
 **/
public class IpCount {
    /**
     * Mapper 程序
     * KEYIN:默认情况下:是mr框架读到的一行内容的起始偏移量Long在hadoop中有比Serializable更为精简的序列化接口 LongWritable
     * VALUEIN:默认情况下 : 是mr框架读到的一行文本内容String同上Text序列号接口
     * KEYOUT:默认情况下:是mr框架经过分布式计算后输出的key单词String
     * VALUEOUT:默认情况下:是mr框架经过分布式计算后输出的value每个单词出现的次数IntWritable
     * @author Administrator
     * 输入LongWritable:行号
     * 输入Text:一行内容
     * 输出Text:单词
     * 输出IntWritable:单词个数
     */
    /**
     * ip--key
     * 出现次数--value
     * 统计日志数据中Top ip以统计各省份PV
     */
     static class IpCountMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
        // ip
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1.将value转换成字符串
            String str = value.toString();
            // 2.进行切割
            String[] fileds = str.split(" ");
            String ip = fileds[0];
            // 3.放入Context中写入Reducer程序
            k.set(ip);
            context.write(k,v);
        }
    }

    /**
     * Reducer程序
     *reducer 输入为mapper 阶段的输出 <Text,IntWritable> <ip,1>
     *reducer 输出为 <Text,IntWritable> <ip,总次数>
     */
    static class IpCountReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /**
             * 一组相同key并发迭代累计总次数 <ip,1> <ip,1>......
             */
            // 统计总次数
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                count += iterator.next().get();
            }
            // reducer写出
            context.write(key,new IntWritable(count));
        }
    }

    /**
     * 运行主程序
     */
    public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取配置对象
        Configuration configuration = new Configuration();
        // 2.通过配置对象获取作业对象
        Job job = Job.getInstance(configuration);
        // 3.指定主程序、mapper类、reducer类
        job.setJarByClass(IpCount.class);
        job.setMapperClass(IpCountMapper.class);
        job.setReducerClass(IpCountReducer.class);
        // 4.设置自动分区算法
        // 5.设置Mapper程序key、value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 6.设置Reducer程序key、value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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