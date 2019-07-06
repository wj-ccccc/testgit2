package com.buba.sparksql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Create by Wj on 2019/6/7 15:32
  */
object SqlWordCount {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName("DataSetWordCount")
      .master("local[*]")
      .getOrCreate()
    val frame: DataFrame = session.read.format("text").load("C:\\Users\\卿九离\\Desktop\\ABC.txt")
    //进行map操作要导入隐式转换
    import session.implicits._

    val abcDataset: Dataset[String] = frame.flatMap(r => r.getAs[String]("value").split(" "))
    //ds  ->  df
    val df: DataFrame = abcDataset.toDF("world")
    //注册视图
    df.createTempView("t_abc")
    val result = session.sql("select world,count(world) count from t_abc group by world order by count desc")


    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    /*
      使用write.jdbc的时候，会在数据库中创建一张相应的表
      mysql
      将boolean---->bit(1)
      如果是string---->text(大文本的类型)
      mode: overwrite ,append
    * */
    result.write.mode("append").jdbc("jdbc:mysql://localhost:3306/hhh?characterEncoding=utf-8", "ppp", properties)
  }

}
