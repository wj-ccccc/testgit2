package com.buba.sparksql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object OldSparkSql2 {
  val getCooection = () => {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/hhh?characterEncoding=utf-8", "root", "root")

  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("OldSparkSql").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val jdbcrdd: JdbcRDD[Row] = new JdbcRDD(
      sc,
      getCooection,
      "select id,xsm,zl,zdj from xiaoshuo where id >= ? and id<=  ?",
      559682,
      583001,
      5,
      rs => {
        val id = rs.getInt("id")
        val xsm = rs.getString("xsm")
        val zl = rs.getString("zl")
        val zdj = rs.getInt("zdj")
        Row(id, xsm, zl, zdj)
      }
    )
    //创建SQLContext
    val sQLContext = new SQLContext(sc)
    //隐式转换
    val schema: StructType = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("xsm", StringType, true),
        StructField("zl", StringType, true),
        StructField("zdj", IntegerType, true)
      )
    )
    //将RDD转换成DataFrame
      val df: DataFrame = sQLContext.createDataFrame(jdbcrdd,schema)
    //将DataDrame注册成临时表
    df.registerTempTable("t_persion")
    //执行sql,sql方法是Transformation不会执行任务只会显示20行
    //  val result: DataFrame = sQLContext.sql("select * from t_persion")
    //  result.show()
    //DSL方法
    df.select("id", "xsm", "zdj").where(df.col("zdj") > 202060000).show()
  }

}
