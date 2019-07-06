package com.buba.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("RDDTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("warn")
    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val RDD0: RDD[Int] = sparkContext.parallelize(array)
    RDD0.map(_ * 2).foreach(println(_))
    println("----------------mapPartitions---map分区----------------")
    val RDD1 = sparkContext.parallelize(array,2)
    /*val unit = RDD1.mapPartitions((x: Iterator[Int]) => {
    println("分区")
      val ints = x.toList.map(_ * 2)
      ints.toIterator
    })*/
    val unit = RDD1.mapPartitions(_.map(x=>x*x))
    unit.foreach(println(_))
    println("----------------mapPartitionsWithIndex---多了个分区号,可以用排序----------------")
    val unit2 = RDD1.mapPartitionsWithIndex((x: Int, data: Iterator[Int]) => {
      println("分区：" + x)
      val ints = data.toList.map(_ * 2)
      ints.toIterator
    })
    unit2.foreach(println(_))
    println("----------------sample----------------")
    //sample(是否放回，每个数据被采样的概率，比例)
    val list = (1 to 100).toList
    val RDD3 = sparkContext.makeRDD(list)
    val RDD4: RDD[Int] = RDD3.sample(false,0.01,0)
    RDD4.foreach(println(_))
    println("----------------takeSample---取num个函数---------")
    //区别
    //takeSample：返回一个array  Action
    //sample：返回一个RDD  Transformation
    val RDD5: Array[Int] = RDD3.takeSample(false,10,0)
    RDD5.foreach(println(_))
    println("-----------groupByKey-------------")
    val list2 = List(("aa",30),("bb",60),("cc",50),("dd",30),("aa",31),("bb",42),("cc",40),("dd",20),("dd",70))
    val RDD6 = sparkContext.makeRDD(list2)
    val gro = RDD6.groupByKey()
    gro.foreach(x=>println(x._1+":"+x._2.toList.mkString(",")))
    gro.foreach(x=>println(x._1+"-平均分-"+x._2.sum.toDouble/x._2.size))
    println("-----------reduceByKey-------------")
    val RDD7 = RDD6.reduceByKey(_ + _)
    RDD7.foreach(println(_))
    println("-----------sortByKey---true:正序,key value 形式的-------")
    RDD7.map(x=>(x._2,x._1)).sortByKey(true).map(x=>(x._2,x._1)).foreach(println(_))
    println("-----------sortBy---自定义形式排序规则----------")
    RDD7.sortBy((x:(String,Int))=>x._2,false).foreach(println(_))
    println("-----------aggregate----------")
    //aggregate : 针对单个元素的RDD 聚合
    //aggregateByKey : groupByKey+aggregate 对每一组的values进行aggregate操作
    //zeroValue: 初始值
    //seqOp:  迭代操作，拿RDD的每个值个和初始值合并
    //combOp: 分区结果数据合并
    println(RDD0.aggregate(0)((_ + _), (_ + _)))
    //平均数
    val RDD11 = RDD0.aggregate((0,0))((u:(Int,Int),x:Int)=>(u._1+x,u._2+1),((x:(Int,Int),y:(Int,Int))=>(x._1+y._1,x._2+y._2)   ))
    println(RDD11)
    println(RDD11._1.toDouble/RDD11._2)
    println("-----------aggregateByKey----------")
    val rdd12: RDD[(String, (Int, Int))] = RDD6.aggregateByKey((0,0))((u:(Int,Int), x:Int)=>(u._1+x,u._2+1),((x:(Int,Int), y:(Int,Int))=>(x._1+y._1,x._2+y._2)))
    rdd12.foreach(x=>println(x._1+" "+x._2._1.toDouble/x._2._2))
    println("-----------combineByKey-计算平均---------")
    val RDD13: RDD[(String, (Int, Int))] = RDD6.combineByKey((x=>(x,1)),((x:(Int,Int), y:Int)=>(x._1+y,x._2+1)),((x:(Int,Int), y:(Int,Int))=>(x._1+y._1,x._2+y._2)))
    RDD13.map((f:(String,(Int,Int)))=>(f._1,f._2._1.toDouble/f._2._2)).foreach(println(_))






















    sparkContext.stop()

  }
}
