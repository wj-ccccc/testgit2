package com.buba.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Create by Wj on 2019/6/10 20:10
  */
object SparkSqlHiveDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlHiveDemo").master("local[*]")
      //启动hive支持
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://master:8020/user/hive/warehouse")
      .getOrCreate()
    spark.sql("show databases").show()
  }
}
