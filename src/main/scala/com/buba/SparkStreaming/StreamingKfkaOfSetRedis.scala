package com.buba.SparkStreaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Create by Wj on 2019/6/12 20:22
  */
object StreamingKfkaOfSetRedis {//设置偏移量 基于Direct(No Receiver)方式
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingKfka")
      //每个分区每秒读的个数  3个分区  100*分区个数*Seconds(2)
    conf .set("spark.streaming.receiver.maxRate","100")
    val sc = new SparkContext(conf)
    val streamingContext = new StreamingContext(sc, Seconds(2))
    //定义kafka参数
    var groupid="c"
    val map = Map[String, Object](
      "bootstrap.servers" -> "admin:9092,admin:9093,admin:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean) //手动提交
    )
    //存储了offset集合 :collection.Map[TopicPartition, Long]
    val offsetsMap = mutable.HashMap[TopicPartition, Long]()
    val jedis = JedisPoolUtil.getJedis()
    val all = jedis.hgetAll(groupid+"-"+"test11")
    //导入语法转换
    import scala.collection.JavaConversions._
      for (a<-all){
        println("a:"+a._1,a._2)
        offsetsMap+=(new TopicPartition("test11",a._1.toInt))->a._2.toLong
      }

    val result: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,
      // PreferBrokers  当 spark计算节点， 和 kafka节点在同一台机器上
      //PreferConsistent   将拉取到的数据均匀的分散到各个节点上
      LocationStrategies.PreferConsistent,
      //订阅主题信息
      ConsumerStrategies.Subscribe(Array("test11"), map,offsetsMap)
    )
    //存到jedis中
    result.foreachRDD(f => {
      //设置偏移量
      val directStream: Array[OffsetRange] = f.asInstanceOf[HasOffsetRanges].offsetRanges
      val rdd: RDD[(String, Int)] = f.map(t => (t.value(), 1)).reduceByKey(_ + _)
      rdd.foreachPartition(c => {
        val jedis = JedisPoolUtil.getJedis()
        c.foreach(t => jedis.hincrBy("string", t._1, t._2))
        jedis.close()
      })
      //提交偏移量
      directStream.foreach(t=>{
        println(t)
        val jedis = JedisPoolUtil.getJedis()
        jedis.hset(groupid+"-"+t.topic,t.partition+"",t.untilOffset+"")
      })
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
