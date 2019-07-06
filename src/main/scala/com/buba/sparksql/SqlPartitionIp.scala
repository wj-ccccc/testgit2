package com.buba.sparksql

import com.buba.spark.IpSpark.ip2Long
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Create by Wj on 2019/6/9 21:26
  */
object SqlPartitionIp {//自定义函数 UDF
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
  def ipparquet()={
    val spark = SparkSession.builder().appName("SqlPartitionIp").master("local[*]").getOrCreate()
    val ipDS: Dataset[String] = spark.read.textFile("C:\\Users\\卿九离\\Desktop\\nn.txt")
    import spark.implicits._
    val mds=ipDS.flatMap(_.split(" "))
    val frame = mds.toDF("ip")
    frame.write.parquet("D:\\asd\\sparkdemo2\\ipparquet")
  }
  def main(args: Array[String]): Unit = {//把要查询的数据已parquet保存到本地
    val spark = SparkSession.builder().appName("SqlPartitionIp").master("local[*]").getOrCreate()
    val ipDS = spark.read.textFile("C:\\Users\\卿九离\\Desktop\\ip.txt")
    import spark.implicits._
    val ipp = ipDS.map(f => {
      val sp = f.split("\\|")
      val ip2 = ip2Long(sp(2))
      val ip3 = ip2Long(sp(3))
      (ip2, ip3, sp(1))
    })
    //将规则数据广播
    val cast= spark.sparkContext.broadcast(ipp.collect())
    //读取parquet数据
    val iparDS = spark.read.parquet("D:\\asd\\sparkdemo2\\ipparquet")
    iparDS.createTempView("t_ipp")
    //自定义函数ipaccess（）
    spark.udf.register("ipaccess",(s: String)=>{
      val cds: Array[(Long, Long, String)] = cast.value
      val result = binarySearch(cds,ip2Long(s))//获取数据
      cds(result)._3
    })
    spark.sql("select ipaccess(ip),count(*) from t_ipp group by ipaccess(ip) " ).show()

  }
  //二分查找，根据ip的阶段获取所在Array的地址
  def binarySearch(line: Array[(Long,Long,String)],ip: Long):Int={
    var num=line.length-1
    var low=0
    while(low<=num){
      val ipnum = (low+num)/2
      if(ip<=line(ipnum)._2 && ip >=line(ipnum)._1){
        return ipnum
      }
      if(ip>line(ipnum)._2){
        low=ipnum+1
      }else{
        num=ipnum-1
      }
    }
    -1
  }
}
