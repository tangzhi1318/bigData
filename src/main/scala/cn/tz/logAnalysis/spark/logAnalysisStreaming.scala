package cn.tz.logAnalysis.spark

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka实时拉取数据实时分析计算然后下沉至MySql
  */
object logAnalysisStreaming {
  def main(args: Array[String]): Unit = {
    // 设置日志打印级别
    LoggerLevels.setStreamingLogLevels()
    val prop = new Properties()
    prop.load(new FileInputStream(new File("C:\\Users\\Administrator\\Log_Analysis\\src\\main\\resources\\kafka.properties")))
    val sparkConf = new SparkConf().setAppName("logAnalysisStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    // 检查点存放文件地址
    ssc.checkpoint("d://ck")
    // 获取kafka主题(模拟kafka中有多个kafka主题)
    val topicMap = prop.getProperty("topics").split(",").map((_, prop.getProperty("numThreads").toInt)).toMap
    // 从kafka定时批量获取新闻数据
    val line = KafkaUtils.createStream(ssc,prop.getProperty("zkQuorum"),prop.getProperty("group"),topicMap,StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    /**
      * spark 实时从kafka上拉取数据实时分析计算存入MySql数据库
      */

  }
}
