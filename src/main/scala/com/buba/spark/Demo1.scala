package com.buba.spark

object Demo1 {
  def main(args: Array[String]): Unit = {
 /*   val conf = new SparkConf().setAppName("helloSpark").setMaster("local")
    val sc = new SparkContext(conf);
    sc.setLogLevel("warn")
    val value = sc.textFile("C:\\Users\\卿九离\\Desktop\\test.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //value.collect.foreach(println(_))
      value.saveAsTextFile("C:\\Users\\卿九离\\Desktop\\test2")//保存到指定路径
     sc.stop()*/
 val ipNum = ip2Long("127.0.0.1")
    println(ipNum)
  }
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
