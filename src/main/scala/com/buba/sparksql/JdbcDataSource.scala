package com.buba.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
/**
  * Create by Wj on 2019/6/8 21:16
  */
object JdbcDataSource {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName("SparkSqlDataSet")
      .master("local[*]")
      .getOrCreate()

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    val frame: DataFrame =session.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://localhost:3306/hhh?characterEncoding=utf-8",
        "driver"->"com.mysql.jdbc.Driver",
        "dbtable"->"room",
        "user"->"root",
        "password"->"root"
      )
    ).load()
  /*  val frame: DataFrame = session.read.jdbc(
      "jdbc:mysql://localhost:3306/hhh?characterEncoding=utf-8",
      "room",
      properties
    )*/
    frame.where(frame.col("id")>=3).write.mode("append").jdbc("jdbc:mysql://localhost:3306/hhh?characterEncoding=utf-8", "room", properties)

  }
}
