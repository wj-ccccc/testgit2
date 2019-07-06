package com.buba.spark

import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(args(0)).setMaster(args(1))
    val sc = new SparkContext(conf);
    sc.setLogLevel("warn")
    val value = sc.textFile(args(2)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //value.collect.foreach(println(_))
    value.saveAsTextFile(args(3))//保存到指定路径
    //flatMap:将多个list合并成一个，map:根据函数规则对List中的每个参数做映射,(_,1):将数据变成元祖,reduceByKey:进行计算,collect:在spark集群是运行不加collect会将结果分散到各个节点上因为foreach是在主控程序，main方法里面执行的但是println(_)是在对应的task里面执行，collect表示将结果收集到主控程序中

    sc.stop()
  }
}
