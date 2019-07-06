package com.buba.SparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by Wj on 2019/6/11 15:05
  */
object StreamingWordCount {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val spark = new SparkContext(sparkConf)
    // Streaming 程序的新入口 批次时间  必须设置在StreamingContext中
    val streaming: StreamingContext = new StreamingContext(spark, Seconds(3))
    //从socket中读取数据
    val dsData: ReceiverInputDStream[String] = streaming.socketTextStream("master", 9999)
    val result: DStream[(String, Int)] = dsData.flatMap(_.split(" +")).map((_, 1)).reduceByKey(_ + _)

    //result.count().print() //每个RDD元素数量
    //result.countByValue().print()//((a,4),1) ((b,3),1) ((c,1),1)
    val transform: DStream[Int] = result.transform(r=>r.map(_._2))//获取value
    transform.print()
    //启动任务
    streaming.start()
    //把线程挂起
    streaming.awaitTermination()
  }
}
