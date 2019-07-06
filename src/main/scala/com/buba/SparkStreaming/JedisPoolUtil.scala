package com.buba.SparkStreaming

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Create by Wj on 2019/6/13 16:06
  */
object JedisPoolUtil {
  private val config = new JedisPoolConfig()
  config.setMaxTotal(2000) // 最大连接数
  config.setMaxIdle(5) // 最大的空闲的连接池数量

  // 私有化连接池
  private[this] val pool = new JedisPool(config, "master")

  // 公有的获取连接的方式
  def getJedis() = {
    pool.getResource
  }
}
