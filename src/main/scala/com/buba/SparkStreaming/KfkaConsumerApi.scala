package com.buba.SparkStreaming

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * Create by Wj on 2019/6/13 9:39
  */
object KfkaConsumerApi {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "admin:9092,admin:9093,admin:9094")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    //指定消费的组
    props.setProperty("group.id","xx")
    //指定从哪儿开始读
    props.setProperty("auto.offset.reset","earliest")//从偏移量0开始读
  //  props.setProperty("auto.offset.reset","latest")//默认从最新的读
    val con = new KafkaConsumer[String, String](props)

    con.subscribe( util.Arrays.asList("test11"))
    while (true){
      //设置超时时间
      val cr: ConsumerRecords[String, String] = con.poll(2000)
      val pp: util.Iterator[ConsumerRecord[String, String]] = cr.iterator()
      while (pp.hasNext){
       // Thread.sleep(1000)
        val result: ConsumerRecord[String, String] = pp.next()
        println(result)
      }
    }


  }
}
