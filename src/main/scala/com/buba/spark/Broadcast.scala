package com.buba.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Broadcast {//广播
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Broadcast").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("warn")
    val broad: RDD[(String, String)] = sparkContext.textFile("C:\\Users\\卿九离\\Desktop\\broad.txt").
     map(f => (f.split(" ")(0), f.split(" ")(1)))
    val ruu: collection.Map[String, String] = sparkContext.textFile("C:\\Users\\卿九离\\Desktop\\dd.txt").
      map(f => (f.split(" ")(0), f.split(" ")(1))).collectAsMap()
    val rules= sparkContext.broadcast(ruu)

    broad.map(f=>
      (rules.value(f._1),f._2)
    ).foreach(print(_))



  }
}
