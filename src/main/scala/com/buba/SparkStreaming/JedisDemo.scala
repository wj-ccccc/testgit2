package com.buba.SparkStreaming

import redis.clients.jedis.Jedis

/**
  * Create by Wj on 2019/6/12 15:50
  */
object JedisDemo {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("master", 6379)
    val str = jedis.get("a")
    val p = jedis.ping()
    println(str, p)
    jedis.close()
  }
}
