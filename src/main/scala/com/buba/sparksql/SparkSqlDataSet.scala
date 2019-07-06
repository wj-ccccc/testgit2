package com.buba.sparksql

import java.sql.DriverManager
import java.util.Properties

import com.buba.spark.IpSpark.ip2Long
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Create by Wj on 2019/6/7 10:24
  */
object SparkSqlDataSet {//StructType方式



  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName("SparkSqlDataSet")
      .master("local[*]")
      .getOrCreate()
    //从文件中读取
    val TRdd = session.sparkContext.textFile("C:\\Users\\卿九离\\Desktop\\ip.txt").map(f => {
      val sp = f.split("\\|")

      Row(sp(1), sp(2))
    })
    //单个的StructField写法
    //StructType(StructField("ip1",StringType)::Nil)
    val schema=StructType(
      List(
        StructField("ip1",StringType,true),
        StructField("ip2",StringType,true)
        //StructField("zl",StringType,true),
       // StructField("zdj",IntegerType,true)
      )
    )
    //创建df 一种方法，toDF（）
    val df: DataFrame = session.createDataFrame(TRdd,schema)//TRdd必须是Row类型
    //临时表
    df.createTempView("t_xs")
    val result = session.sql("select ip1,ip2 from t_xs")
    result.show()
  }
  
}

