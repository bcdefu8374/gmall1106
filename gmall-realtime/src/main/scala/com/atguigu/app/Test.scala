package com.atguigu.app

import com.atguigu.util.RedisUtil
import redis.clients.jedis.Jedis

/**
 * @author chen
 * @topic
 * @create 2020-11-06
 */
object Test {

  def main(args: Array[String]): Unit = {

    val jedisClient: Jedis = RedisUtil.getJedisClient
    jedisClient.sadd("a", "aaaa")

    jedisClient.close()
  }


}