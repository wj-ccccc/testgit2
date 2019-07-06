package com.buba.SparkStreaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
  * Create by Wj on 2019/6/12 20:22
  */
object StreamingKfka {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {//设置偏移量
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKfka")
    val sc = new SparkContext(conf)
    val streamingContext = new StreamingContext(sc, Seconds(2))
    //定义kafka参数
    val map = Map[String, Object](
      "bootstrap.servers" -> "admin:9092,admin:9093,admin:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "c",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean) //手动提交
    )
    val result: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,
      // PreferBrokers  当 spark计算节点， 和 kafka节点在同一台机器上
      //PreferConsistent   将拉取到的数据均匀的分散到各个节点上
      LocationStrategies.PreferConsistent,
      //订阅主题信息
      ConsumerStrategies.Subscribe(Array("wc"), map)
    )
    //存到jedis中
    result.foreachRDD(f => {
      //设置偏移量
      val directStream: Array[OffsetRange] = f.asInstanceOf[HasOffsetRanges].offsetRanges
      //分区的数据
     // val poffsetrange = directStream(TaskContext.get().partitionId())
      directStream.foreach(println(_))
      val rdd: RDD[(String, Int)] = f.map(t => (t.value(), 1)).reduceByKey(_ + _)
      rdd.foreachPartition(c => {
        val jedis = JedisPoolUtil.getJedis()
        c.foreach(t => jedis.hincrBy("string", t._1, t._2))
        jedis.close()
      })
      //提交偏移量
      result.asInstanceOf[CanCommitOffsets].commitAsync(directStream)
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
