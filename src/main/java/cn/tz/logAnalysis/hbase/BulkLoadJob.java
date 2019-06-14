package cn.tz.logAnalysis.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName BulkLoadJob
 * @Description 从HDFS上快速导入数据至Hbase中
 * @Author Administrator
 * @Version 1.0
 **/
public class BulkLoadJob {
    /**
     * 原理：BulkLoad不会写WAL，也不会产生flush以及split。
     *     利用HBase数据按照HFile格式存储在HDFS的原理，
     *     使用Mapreduce直接生成HFile格式文件后，RegionServers再将HFile文件移动到相应的Region目录下
     */
    static Logger logger = LoggerFactory.getLogger(BulkLoadJob.class);
    public static class BulkLoadMapper extends Mapper <LongWritable, Text, ImmutableBytesWritable,Put> {
        private static final byte[] FAMILY_BYTE = Bytes.toBytes("info");
        private final String[] nameTable = {"ip", "else1", "else2", "time" ,"time_area","http_method","resource","http_version","http_status","fluw"};
//        private long count = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fileds = line.split(" ");
            /** 测试源数据是否规则
             *count += 1;
             *if (fileds.length != 10) {
             *System.out.println(fileds.length + " " + count);}
             */
//                byte[] flag = Bytes.toBytes(String.valueOf(count += 1));
                // 生成Put对象
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(getRowkey(fileds[0].toString(),fileds[3].toString()));
                Put put = new Put(getRowkey(fileds[0].toString(),fileds[3].toString()));
                for ( int i = 0 ; i < fileds.length ; i++) {
                    put.add(FAMILY_BYTE,Bytes.toBytes(nameTable[i]),Bytes.toBytes(fileds[i]));
                }
                context.write(rowKey,put);
            }
    }
    /**
     * Rowkey设计
     */
    public static byte[] getRowkey(String ip,String time) {
        /**
         * *关键：Rowkey的设计不良容易导致热点发生在大量的client直接访问集群的一个或极少数个节点（访问可能是读，写或者其他操作）
         * 加盐、哈希、反转等方法解决 Hbase存储、访问、DDL时的热点现象（当一大波数据流向同一节点，导致该节点不可用）
         */
        // 1.获取uuid
        String preUuid2 = UUID.randomUUID().toString();
        // 2.去掉"-"符号
        String changUuid = preUuid2.substring(0,8)+preUuid2.substring(9,13)+preUuid2.substring(14,18)
                +preUuid2.substring(19,23)+preUuid2.substring(24);
        // 3.根据当前业务日期生成时间戳
        String[] split = time.split(":");
        String realTime = split[1] + ":" + split[2] + ":" + split[3];
        String date = "2013-05-30 "+realTime+"";
        long TimeMillis = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(date, new ParsePosition(0)).getTime() / 1000;
        String rowkey = changUuid + "_" + ip + "_" + TimeMillis;
        return Bytes.toBytes(rowkey);
    }

    /**
     * 不需要写reducer方法，不对数据做任何的计算操作
     */

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigData");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Job job = Job.getInstance(conf, "hbase-bulkload");
        job.setJarByClass(BulkLoadJob.class);
        // set input path
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        // set map
        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        HTable table = new HTable(conf, args[0]);
        HFileOutputFormat.configureIncrementalLoad(job, table);
        // job是否提交成功
        boolean res = job.waitForCompletion(true);
        // 将Hfile导入到hbase表中 相当于shell中的 completebulkload
        LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
        load.doBulkLoad(new Path(args[2]), table);
        return res ? 0 : 1;
    }
    /**
     * HFile转换(Hbase装载数据)
     */
    public static void main(String[] args) throws Exception {
        args = new String[] { "Log_analysis1", "hdfs://192.168.25.136:9000/log_analysis.log5",
                "hdfs://192.168.25.136:9000/Hfile" };
        int status = new BulkLoadJob().run(args);
        System.exit(status);
    }
}