package cn.tz.logAnalysis.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试Spark读取Hbase数据
  */
object TestHbaseSpark {
  def main(args: Array[String]): Unit = {
    // 设置日志打印级别
    LoggerLevels.setStreamingLogLevels()
    // 1.创建Spark连接
    val sparkConf = new SparkConf().setAppName("TestHbaseSpark").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val tableName = "test_logs"
    val conf = HBaseConfiguration.create()
    // 2.设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","bigData")
    // 3.设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    // 4.如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }
    // 5.读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = hBaseRDD.count()
    println(count)
    hBaseRDD.foreach{case (_,result) =>{
      // 6.获取行键
      val Rowkey = Bytes.toString(result.getRow)
      // 7.通过列族和列名获取列
      // "ip", "else1", "else2", "time" ,"time_area","http_method","resource","http_version","http_status","fluw"
      val ip = Bytes.toString(result.getValue("info".getBytes,"ip".getBytes))
      val else1 = Bytes.toString(result.getValue("info".getBytes,"else1".getBytes))
      val else2 = Bytes.toString(result.getValue("info".getBytes,"else2".getBytes))
      val time = Bytes.toString(result.getValue("info".getBytes,"time".getBytes))
      val time_area = Bytes.toString(result.getValue("info".getBytes,"time_area".getBytes))
      val http_method = Bytes.toString(result.getValue("info".getBytes,"http_method".getBytes))
      val resource = Bytes.toInt(result.getValue("info".getBytes,"resource".getBytes))
      val http_version = Bytes.toString(result.getValue("info".getBytes,"http_version".getBytes))
      val http_status = Bytes.toString(result.getValue("info".getBytes,"http_status".getBytes))
      val fluw = Bytes.toString(result.getValue("info".getBytes,"fluw".getBytes))
      println("Rowkey:" + Rowkey  + " " + "ip:" + ip + " " + "else1:" + else1 + " " + "else2:" + else2 + "time:" + time + "time_area:" + time_area
        + " " + "http_method:" + http_method + " " + "resource:" + resource + " " + "http_version:" + http_version + " " + "http_status:" + http_status + " "
        + "fluw" + fluw)
    }}
    sc.stop()
    admin.close()
  }
}
