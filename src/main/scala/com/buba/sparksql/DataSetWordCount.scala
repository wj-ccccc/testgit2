package com.buba.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Create by Wj on  2019/6/7 15:14
  */
object DataSetWordCount {//DataSet
  def main(args: Array[String]): Unit = {
    val session2 = SparkSession
      .builder()
      .appName("DataSetWordCount")
      .master("local[*]")
      .getOrCreate()
   // session2.read.parquet("C:\\Users\\卿九离\\Desktop\\parquet").show()
    var abcRDD: Dataset[String] = session2.read.textFile("C:\\Users\\卿九离\\Desktop\\ABC.txt")
    //导入session中的隐式转换上面的SparkSession
    import session2.implicits._
    val abcRdd: Dataset[String] = abcRDD.flatMap(_.split(" "))
    //DSL方法:
   /* //为了agg中的聚合函数可以使用，导入soark sql中的函数
    import org.apache.spark.sql.functions._
    abcRdd.groupBy($"value"as "word").agg(count("*") as "count").sort($"count"desc).show()
*/
    //sql方法
 //   val renamed: DataFrame = abcRdd.withColumnRenamed("value","world")
   val renamed: DataFrame =abcRdd.toDF("world")
    renamed.createTempView("t_abc")
    val result = session2.sql("select world,count(world) count from t_abc group by world order by count desc")
    result.write.parquet("C:\\Users\\卿九离\\Desktop\\parquet")

   /* abcRdd.createTempView("t_abc")
    val result = session2.sql("select value,count(value) count from t_abc group by value order by count desc")
*/
  }
}
