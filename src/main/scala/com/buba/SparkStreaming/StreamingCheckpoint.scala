package com.buba.SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * Create by Wj on 2019/6/11 20:57
  */
object StreamingCheckpoint {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val chack = () =>{
    val sparkConf = new SparkConf()
      .setAppName("StreamingSql")
      .setMaster("local[*]")
    val spark = new SparkContext(sparkConf)
    // Streaming 程序的新入口 批次时间  必须设置在StreamingContext中
    val streaming: StreamingContext = new StreamingContext(spark, Seconds(4))

    streaming.checkpoint("streami123ng-cak")
    //从socket中读取数据
    val dsData: ReceiverInputDStream[String] = streaming.socketTextStream("master", 9999)
    val update=(s:Seq[Int], o:Option[Int])=>{
      Some(s.sum+o.getOrElse(0))
    }
    val result=dsData.flatMap(_.split(" +")).map((_, 1)).updateStateByKey(update)
    result.print()
    streaming
  }
  def main(args: Array[String]): Unit = {
    val ssc=StreamingContext.getOrCreate("streami123ng-ck",chack)

    //启动任务
    ssc.start()
    //把线程挂起
    ssc.awaitTermination()
  }
}
