package com.buba.SparkStreaming

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * Create by Wj on 2019/6/12 11:08
  */
object StreamingCheckpointSql {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val spark = new SparkContext(sparkConf)
    // Streaming 程序的新入口 批次时间  必须设置在StreamingContext中
    val streaming: StreamingContext = new StreamingContext(spark, Seconds(3))
    //从socket中读取数据
    val dsData: ReceiverInputDStream[String] = streaming.socketTextStream("master", 9999)
    val result: DStream[(String, Int)] = dsData.flatMap(_.split(" +")).map((_, 1)).reduceByKey(_ + _)
    result.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){

      rdd.foreachPartition(f => {
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/hhh?characterEncoding=utf-8", "root", "root")
        val ex = conn.prepareStatement(s"create table if not exists streaming(word varchar(20),conts int)")
        ex.execute()
        ex.close()
        f.foreach(t=>{
          val word = t._1
          val conts = t._2
          val query = conn.prepareStatement(s"select conts from streaming where word=?")
          query.setString(1,word)
          val qq = query.executeQuery()
          if(qq.next()){
            val old = qq.getInt("conts")
            val st = conn.prepareStatement(s"update streaming set conts=? where word=?")
            st.setInt(1,old+conts)
            st.setString(2,word)
            st.execute()
          }else{
            val insert = conn.prepareStatement(s"insert into streaming values(?,?)")
            insert.setString(1,word)
            insert.setInt(2,conts)
            insert.execute()
          }
        })
        conn.close()
      })
      }
    })


    result.print()
    //启动任务
    streaming.start()
    //把线程挂起
    streaming.awaitTermination()
  }
}
