package com.buba.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
  * DateTime: 2019/7/6 13:30
  */
object FPGrowthDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("lll").setMaster("local[*]")
    val spark = new SparkContext(conf)
    val txt: RDD[Array[String]] = spark.textFile("D:\\asd\\sparkdemo2\\src\\main\\resources\\fpgrowth.txt").map(_.split(" ")).cache()
    println("样本数量："+ txt.count())
    //最小支持度
    val minSupport = 0.5
    //并行度
    val NumPartitions=2
    val fp = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(NumPartitions)
      .run(txt)
    println("经常一起购买的数量为："+fp.freqItemsets.count())
    fp.freqItemsets.collect().foreach { f =>
      if(f.items.length>=3)println(f.items.mkString("[", ",", "]")+ ","+ f.freq)
    }
    spark.stop()
  }
}
