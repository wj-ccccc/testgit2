package com.buba.SparkStreaming

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * Create by Wj on 2019/6/13 9:39
  */
object KfkaConsumerApi2 {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "admin:9092,admin:9093,admin:9094")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    props.setProperty("group.id", "112121")
    props.setProperty("group.name", "" +
      "") // 可选
    props.setProperty("auto.offset.reset","earliest")


    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("test11"))
    while (true) {
      // 延迟时间
      val poll: ConsumerRecords[String, String] = consumer.poll(2000)

      val iterator: util.Iterator[ConsumerRecord[String, String]] = poll.iterator()
      while (iterator.hasNext()) {
        val record: ConsumerRecord[String, String] = iterator.next()

        println(record)
        //        println(record.partition(),record.value())
      }
    }

  }
}
