package com.buba.SparkStreaming
/**
  * ━━━━━━神兽出没━━━━━━
  * 　　　┏┓　　　┏┓
  * 　　┏┛┻━━━┛┻┓
  * 　　┃　　　　　　　┃
  * 　　┃　　　━　　　┃
  * 　　┃　┳┛　┗┳　┃
  * 　　┃　　　　　　　┃
  * 　　┃　　　┻　　　┃
  * 　　┃　　　　　　　┃
  * 　　┗━┓　　　┏━┛
  * 　　　　┃　　　┃神兽保佑, 永无BUG!
  * 　　　　┃　　　┃Code is far away from bug with the animal protecting
  * 　　　　┃　　　┗━━━┓
  * 　　　　┃　　　　　　　┣┓
  * 　　　　┃　　　　　　　┏┛
  * 　　　　┗┓┓┏━┳┓┏┛
  * 　　　　　┃┫┫　┃┫┫
  * 　　　　　┗┻┛　┗┻┛
  * ━━━━━━感觉萌萌哒━━━━━━
  * Module Desc:
  * User: wujian
  * DateTime: 2019/6/19 11:17
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
object TransformDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val config = new SparkConf().setAppName("TransformDemo").setMaster("local[2]")
    val ssc = new StreamingContext(config, Seconds(2))
    //定义黑名单数组
    val blackList = Array(("tom", true), ("jim", true))
    //黑名单RDD
    val blackListRDD = ssc.sparkContext.parallelize(blackList)
    //定义一个socket输入流
    ssc.socketTextStream("master", 9999).map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val clickDate = fields(1)
      (name, clickDate)
    }).transform(rdd => {
      //进行黑名单过滤，拿到发过来的数据和黑名单数据进行join连接
      //(tom,2017-03-02) leftOuterJoin (tom,true)  ===> (tom,(2017-03-02,Some(true)))
      rdd.leftOuterJoin(blackListRDD).filter(tuple => {
        //过滤掉黑名单里面的数据，isEmpty判断是否为null，如果是null返回true
        //过滤出这样的数据(jom,(2017-09-09,None))
        if (tuple._2._2.isEmpty) true else false
      })
    }).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
