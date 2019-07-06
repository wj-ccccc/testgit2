package com.buba.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

//自定义分区
object PartitionDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("PartitionDemo").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("warn")
    val teacher: Array[((String, String), Int)] = Array((("java", "laowang"), 4), (("java", "laoliu"), 10),
      (("java", "laolv"), 5), (("python", "laozhang"), 7), (("python", "laoqin"), 4))

    val teacherRDD: RDD[((String, String), Int)] = sparkContext.makeRDD(teacher)

    //teacherRDD这个RDD会反复使用，放到缓存中
    teacherRDD.cache()
    //第一种 不知道有多少学科
    //获取所有学科,distinct: 去重,collect: 转换成Array
    val SubjectTeacher: Array[String] = teacherRDD.map(_._1._1).distinct().collect()
    //因为getPartition(key: Any) 是根据Key分区的所以把学科放到key
    val subject = teacherRDD.map(f => (f._1._1, (f._1._2, f._2)))
    //partitionBy(自定义分区类)
    val subp: RDD[(String, (String, Int))] = subject.partitionBy(new SubjectPartitioner(SubjectTeacher))
    //排序先转成list然后排序然后转回去,   reversen 逆序，sortBy默认正序，这的sortBy是scala的不能在里面写false
    val result: RDD[(String, (String, Int))] = subp.mapPartitions(_.toList.sortBy(_._2._2).reverse.iterator)
/*    //第二种 知道有多少学科，.reduceByKey(自定义分区,条件)
    val partitioner = new SubjectPartitioner2
    val result = teacherRDD.partitionBy(partitioner).mapPartitions(_.toList.sortBy(_._2).reverse.iterator)*/
    // println(result.collect().toBuffer) 打印的2种写法
    result.foreach(println(_))
  }
}

class SubjectPartitioner2 extends Partitioner {
  //从文件读取科目信息（科目->设置的分区）
  val map = Map("java" -> 1, "python" -> 2)

  override def numPartitions: Int = map.size+1

  //asInstanceOf: 强转成key的数据格式
  override def getPartition(key: Any): Int = {
    val k: (String, String) = key.asInstanceOf[Tuple2[String, String]]
    map.getOrElse(k._1, 0)
  }
}

class SubjectPartitioner(a: Array[String]) extends Partitioner {
  //定义分区规则
  val map = new mutable.HashMap[String, Int]()
  //因为要设定一个默认的分区所以分区从1开始
  var i = 1
  for (sa <- a) {
    map += (sa -> i)
    i += 1
  }

  //有几个分区
  override def numPartitions: Int = a.length + 1

  //根据传入的key决定数据传入那个分区,getOrElseUpdate(key,如果没有获取到)
  //map.get()不能用，因为 如果key没有找到分区要设定一个默认的分区
  override def getPartition(key: Any): Int = map.getOrElseUpdate(key.toString, 0)
}
