package com.buba.SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by Wj on 2019/6/11 15:05
  */
object StreamingWordCount2 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val spark = new SparkContext(sparkConf)
    // Streaming 程序的新入口 批次时间  必须设置在StreamingContext中
    val streaming: StreamingContext = new StreamingContext(spark, Seconds(3))
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    //从socket中读取数据
    val dsData: ReceiverInputDStream[String] = streaming.socketTextStream("master", 9999)
    import session.implicits._
    dsData.foreachRDD(rdd=>{
      val ds: Dataset[String] = session.createDataset(rdd)
      val rdd2 : Dataset[String]= ds.flatMap(_.split(" +"))
      //createOrReplaceTempView:如果有表就不创建
      rdd2.createOrReplaceTempView("test2")
      session.sql("select value,count(value) count from test2 group by value").show()
    })
   /* DStream = RDD + 时间戳
      dsData.foreachRDD((rdd,time)=>{
      val rdd2: RDD[(String, Int)] = rdd.flatMap(_.split(" +")).map((_, 1)).reduceByKey(_ + _)
      rdd2.foreach(println(_,time))
    })*/
    //启动任务
    streaming.start()
    //把线程挂起
    streaming.awaitTermination()
  }
}
