package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chen
 * @topic
 * @create 2020-11-10
 */
//将用户表新增及变化数据缓存
object UserApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //2.创建streamingcontext
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //3.消费kafka用户数据
    val orderUserDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_USER_INFO,ssc)

    //测试
   /* orderUserDStream.foreachRDD( rdd => {
      rdd.foreach( record => println(record.value()))
    })*/

    //取出value值，才是真正的数据
    val userJsonDStream: DStream[String] = orderUserDStream.map(_.value())

    //将数据写入redis
    userJsonDStream.foreachRDD( rdd => {
      rdd.foreachPartition(iter => {
        //1.获取连接
        val jedisClient = RedisUtil.getJedisClient
        //写入redis
        iter.foreach( userJson => {
          //json字符串转化为样例类
          val userInfoJson = JSON.parseObject(userJson,classOf[UserInfo])
          //3.写出去
          val redisKey = s"UserInfo:${userInfoJson.id}"

          //客户端获取
          jedisClient.set(redisKey,userJson)
        } )
        //3.归还连接
        jedisClient.close()
      })
    })


    //开启
    ssc.start()
    ssc.awaitTermination()
  }

}
