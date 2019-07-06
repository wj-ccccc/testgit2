package com.buba.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * ━━━━━━神兽出没━━━━━━
  * 　　　┏┓　　　┏┓
  * 　　┏┛┻━━━┛┻┓
  * 　　┃　　　　　　　┃
  * 　　┃　　　━　　　┃
  * 　　┃　┳┛　┗┳　┃
  * 　　┃　　　　　　　┃
  * 　　┃　　　┻　　　┃
  * 　　┃　　　　　　　┃
  * 　　┗━┓　　　┏━┛
  * 　　　　┃　　　┃神兽保佑, 永无BUG!
  * 　　　　┃　　　┃Code is far away from bug with the animal protecting
  * 　　　　┃　　　┗━━━┓
  * 　　　　┃　　　　　　　┣┓
  * 　　　　┃　　　　　　　┏┛
  * 　　　　┗┓┓┏━┳┓┏┛
  * 　　　　　┃┫┫　┃┫┫
  * 　　　　　┗┻┛　┗┻┛
  * ━━━━━━感觉萌萌哒━━━━━━
  * Module Desc:
  * User: wujian
  * DateTime: 2019/7/6 14:39
  */
object ALSDemo {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("als").setMaster("local[3]")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val lines = session.sparkContext.textFile("D:\\asd\\sparkdemo2\\src\\main\\resources\\als.txt")

    val trainRDD = lines.map {
      lines =>
        val arr = lines.split(",")
        Rating(arr(0).toInt, arr(1).toInt, arr(2).toDouble)
    }
    // 训练数据生成模型
    /*
    rating：由用户-物品矩阵构成的训练集
    rank：隐藏因子的个数
    numIterations: 迭代次数 iterations
    lambda：正则项的惩罚系数 默认0.01
    alpha： 置信参数 默认 -1
    */
    val model = ALS.train(trainRDD, 10, 10)
    // 使用原始数据生成测试数据
    val testRDD = trainRDD.map {
      case Rating(uid, pid, score) => (uid, pid)//如果符合Rating(uid, pid, score)这种格式
    }
    // val predict = model.predict(testRDD)
    // predict.foreach(println)
    // 推荐n件商品给指定id的用户（给id为1的用户推荐五件商品）
    val predictProducts = model.recommendProducts(1, 5)
    println("==推荐n件商品给指定id的用户==")
    predictProducts.foreach {
      case Rating(uid, pid, score) => println("UID:" + uid + ",SCORE:" + pid, score)
    }
    // 推荐指定id的商品给n个用户（id为2的商品推荐给三个用户）
    val predictUsers = model.recommendUsers(2, 3)
    println("\r\n==推荐指定id的商品给n个用户==")
    predictUsers.foreach {
      case Rating(uid, pid, score) => println("UID:" + uid + ",SCORE:" + pid, score)
    }
    // 推荐n件商品给所有用户（四件商品）
    val predictProductsForUsers: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(4)
    predictProductsForUsers.foreach {
      t =>

        println("\r\n向id为：" + t._1 + "的用户推荐了以下四件商品：")
      /*  for (i <- 0 until t._2.length) {
          println("UID:" + t._2(i).user + ",PID:" + t._2(i).product + ",SCORE:" + t._2(i).rating)
        }*/
        t._2.foreach{
          case Rating(uid, pid, score) => println("UID:" + uid + ",SCORE:" + pid, score)
        }
    }
    // 推荐n个用户给所有商品（三个用户）
    val predictUsersForProducts = model.recommendUsersForProducts(3)
    predictUsersForProducts.foreach {
      t =>
        println("\r\n向id为：" + t._1 + "的商品推荐了以下三个用户：")
        for (i <- 0 until t._2.length) {//1 until 10 1到9的值，上限是9
          println("UID:" + t._2(i).user + ",PID:" + t._2(i).product + ",SCORE:" + t._2(i).rating)
        }
       /* t._2.foreach{
          case Rating(uid, pid, score) => println("UID:" + uid + ",SCORE:" + pid, score)
        }*/

    }
  }
}
