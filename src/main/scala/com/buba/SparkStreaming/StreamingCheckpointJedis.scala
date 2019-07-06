package com.buba.SparkStreaming

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import redis.clients.jedis.Jedis

/**
  * Create by Wj on 2019/6/12 16:06
  */
object StreamingCheckpointJedis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val spark = new SparkContext(sparkConf)
    // Streaming 程序的新入口 批次时间  必须设置在StreamingContext中
    val streaming: StreamingContext = new StreamingContext(spark, Seconds(3))
    //从socket中读取数据
    val dsData: ReceiverInputDStream[String] = streaming.socketTextStream("master", 9999)
    val result: DStream[(String, Int)] = dsData.flatMap(_.split(" +")).map((_, 1)).reduceByKey(_ + _)
    result.foreachRDD(rdd=>{
      rdd.foreachPartition(f=>{
    //    val jedis = new Jedis("master", 6379)
        val jedis = JedisPoolUtil.getJedis()
        f.foreach(t=>jedis.hincrBy("string",t._1,t._2))
        jedis.close()
      })
    })


    result.print()
    //启动任务
    streaming.start()
    //把线程挂起
    streaming.awaitTermination()
  }
}
