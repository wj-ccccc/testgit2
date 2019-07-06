package com.buba.SparkStreaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
  * Create by Wj on 2019/6/13 9:39
  */
object KfkaProApi {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "admin:9092,admin:9093,admin:9094")
    props.setProperty("key.serializer", classOf[StringSerializer].getName)
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val pro = new KafkaProducer[String, String](props)
    for (i <- 0 to 1000) {
      Thread.sleep(1000)
      //如果没有指定分区就按照默认的轮询方式 0 1 2 0 1 2
      //如果指定的分区就按照指定的分区进行
      //指定了可以就按照可以的hashcode来分区
      //val msg1 = new ProducerRecord[String, String]("wc", "hello"+i)
      val p=i%3
      val wrold: Int = Random.nextInt(26)+'a'
      println(wrold)
      val msg = new ProducerRecord[String, String]("test11", p,"",wrold+"")
      pro.send(msg)
    }
    println("ok")
  }
}
