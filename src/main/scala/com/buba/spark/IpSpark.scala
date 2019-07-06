package com.buba.spark

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpSpark {//broadcast（广播） ip实例,并且放到数据库中
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("IpSpark").setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ipRDD: RDD[(Long, Long, String)] = sc.textFile("C:\\Users\\卿九离\\Desktop\\ip.txt").map(f => {
      val sp = f.split("\\|")
      val ip2 = ip2Long(sp(2))
      val ip3 = ip2Long(sp(3))

      (ip2,ip3,sp(1))
    })

    val ipruff = sc.textFile("C:\\Users\\卿九离\\Desktop\\nn.txt").flatMap(_.split(" "))
    val bro = sc.broadcast(ipRDD.collect())

    ipruff.map(f => {
      val bip: Array[(Long, Long, String)] = bro.value
      val ip = ip2Long(f)
      val binc = binarySearch(bip, ip)
      if (binc != -1) {
        (bip(binc)._3, 1)
      }else{
        (f, 1)
      }
    }).reduceByKey(_+_).foreachPartition(f=>{
      dataMySQL(f)
    })

  }
  def dataMySQL(f:Iterator[(String,Int)])={
    //创建jdbc链接
    val con = DriverManager.getConnection("jdbc:mysql://localhost:3306/hhh?characterEncoding=utf-8","root","root")
    con.setAutoCommit(false)//手动提交
    val statement = con.prepareStatement("insert into ip values(?,?)")
    f.foreach(p=>{
      statement.setString(1,p._1)
      statement.setString(2,p._2+"")
      statement.executeUpdate()
    })
    con.commit()
    statement.close()
    con.close()
  }
  //二分查找，根据ip的阶段获取所在Array的地址
  def binarySearch(line: Array[(Long,Long,String)],ip: Long):Int={
    var num=line.length-1
    var low=0;
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
  //ip转换成10进制
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
