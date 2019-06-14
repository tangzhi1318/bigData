package cn.tz.logAnalysis.spark

import java.text.{ParsePosition, SimpleDateFormat}
import java.util.Properties

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark读取Hbase 进行日志数据时间段聚合 再采用SparkSQL写入Mysql
  */
object TimePeriodAggregation {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark连接
    val sparkConf = new SparkConf().setAppName("TimePeriodAggregation").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val tableName = "Log_analysis1"
    val conf = HBaseConfiguration.create()
    // 2.设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "bigData")
    // 3.设置zookeeper连接端口，默认2181=
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
    //通过StrutType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id",StringType,true),
        StructField("time_stamp",LongType,true)
      )
    )
    // 6.将RDD映射到rowRDD
    val rowRDD = hBaseRDD.map(p => Row(
      Bytes.toString(p._2.getRow),
      (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(
        "2013-05-30 " + ((Bytes.toString(p._2.getValue("info".getBytes, "time".getBytes))).split(":"))(1) + ":"
          + ((Bytes.toString(p._2.getValue("info".getBytes, "time".getBytes))).split(":"))(2) + ":"
          + ((Bytes.toString(p._2.getValue("info".getBytes, "time".getBytes))).split(":"))(3) + ""
        , new ParsePosition(0)).getTime / 1000
    ))
    // 7.将schema信息应用到rowRDD上
    val HbaseDataFrame = sqlContext.createDataFrame(rowRDD,schema)
    // 8.创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    // 9.将数据追加到数据库
    HbaseDataFrame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://120.79.35.74:3306/bs?characterEncoding=utf-8", "time",prop)
    sc.stop()
    admin.close()
  }
}