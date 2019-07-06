package com.buba.spark

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCRddDemo {
  val getConnection = () => {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/hhh?characterEncoding=utf-8", "root", "root")
  }


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("JDBCRddDemo").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val jdbcrdd = new JdbcRDD(
      sc,
      getConnection,
      "select id,xsm,zl,zdj from xiaoshuo where id >= ? and id<=  ?",
      559682,
    583001,
      5,
        rs =>{
        val id = rs.getString("id")
        val xsm = rs.getString("xsm")
        val zl = rs.getString("zl")
        val zdj = rs.getInt("zdj")
        (id,xsm,zl,zdj)
      }
    )
    jdbcrdd.sortBy(_._4,false).collect().foreach(println(_))
  }
}
