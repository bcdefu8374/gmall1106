package com.atguigu.app

import java.sql.{Connection, Date}
import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstant
import com.atguigu.util.{JdbcUtil, MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @author chen
 * @topic
 * @create 2020-11-10
 */
//将orderinfo与orderdetail数据 进行双流join，并根据user_id查询redis，补全
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //2.创建streamingcontext
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //3.消费kafka订单以及订单明细主题数据创建流
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO,ssc)

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_DETAIL,ssc)


    //4.将数据转化为样例类对象并转换为kv
    val orderInfoDStream = orderInfoKafkaDStream.map(record => {

      //a.将value的值转化为样例类 ,OrderInfo是在bean中定义的样例类，通过classof拿到，因为里面需要传入一个class
      val orderinfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      //b.取出创建时间yyyy-MM-dd HH:mm:ss
      val dateTimeArr: Array[String] = orderinfo.create_time.split(" ")
      orderinfo.create_date = dateTimeArr(0)
      orderinfo.create_hour = dateTimeArr(1).split(":")(0)

      //c.对手机号进行脱敏
      //获取到 手机号
      val consignee_tel: String = orderinfo.consignee_tel
      //先对手机号进行切分把前四位的和后面的分隔开
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderinfo.consignee_tel = tuple._1 + "*******"

      //返回变换好个结果kafka数据
      (orderinfo.id, orderinfo)
    })

    //订单详情的样例类对象
    val orderToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      //a.转化为样例类
      val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

      //b.返回数据
      (detail.order_id, detail)
    })

    //双流JOIN(普通JOIN)
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
    //    value.print(100)
    //普通的join是只会join到同一批次的orderinfo和orderdetailinfo，但是如果是orderinfo在下一批次过来就会jion不上
    //导致丢失数据

    //使用full join
    val orderfullJoinDetail: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderToDetailDStream)

    //处理join后的数据,用mapPartitions是因为数据还要，如果是foreachPartitions没有返回值，没办法进行接下来的处理
    val noUserSaleDetialDStream: DStream[SaleDetail] = orderfullJoinDetail.mapPartitions(iter => {
      //获取redis连接
      val jedisClient = RedisUtil.getJedisClient
      //创建集合用于存放join上的数据
      val details: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]

      //遍历迭代器的iter 做数据处理
      iter.foreach {
        case ((orderId, (infoOpt, detailOpt))) => {

          val infoRedisKey = s"OrderInfo:$orderId"
          val detailRedisKey = s"DetailInfo:$orderId"

          //如果orderinfo是不为空
          if (infoOpt.isDefined) {
            //取出infoOpt数据
            val orderInfo: OrderInfo = infoOpt.get

            //判断detailOpt
            if (detailOpt.isDefined) {
              //取出detailOpt数据
              val detailInfo: OrderDetail = detailOpt.get

              //创建SaleDetail用于存放关联上的数据
              val saleDetail: SaleDetail = new SaleDetail(orderInfo, detailInfo)

              //迭代器是只使用一次，所以要创建一个集合用于存放关联上的数据
              //定义一个集合用于存放
              details += saleDetail
            }

            //a.2逻辑分支转化json写入缓存
            //将info数据写入redis，给后续的detail数据使用
            // val infoStr: String = JSON.toJSONString(orderInfo)//编译通不过
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
            val orderInfoJson: String = Serialization.write(orderInfo)

            jedisClient.set(infoRedisKey, orderInfoJson) //把orderId存放到redis
            jedisClient.expire(infoRedisKey, 100) //设置超时时间

            //a.3查询缓存中是否有对应的OrderDetail
            if (jedisClient.exists(detailRedisKey)) {
              //判断detailInfo存在，从redis取出detail
              val detailJsonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
              //取出来是一个set集合里存放的json字符串，转化为样例类对象
              detailJsonSet.asScala.foreach(detailJson => {
                //转化为detail的样例类对象
                val detail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])

                //创建SaleDetail存放关联上的字段
                val detail1 = new SaleDetail(orderInfo, detail)
                details += detail1
              })
            }
            //判断orderinfo是为空
          } else {
            //获取detailOpt数据
            val orderDetail: OrderDetail = detailOpt.get

            //判断缓存中是否有对应的orderinfo
            if (jedisClient.exists(infoRedisKey)) {
              //取出redis里面的orderInfo数据
              val infoJson: String = jedisClient.get(infoRedisKey)

              //获取到的数据 转化为样例类
              val orderInfo: OrderInfo = JSON.parseObject(infoJson, classOf[OrderInfo])
              //创建SaleDetail存放集合
              details += new SaleDetail(orderInfo, orderDetail)

            } else {
              //转化为json写入缓存
              implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
              val detailStr = Serialization.write(orderDetail)

              jedisClient.set(infoRedisKey, detailStr) //把orderId存放到redis
              jedisClient.expire(infoRedisKey, 100) //设置超时时间
            }
          }

        }
      }
      //归还连接
      jedisClient.close()

      //最终返回值
      details.iterator

    })

    //7.根据UserId查询Redis中数据，补全用户信息,连接第三方框架，用mapPartition
    val saleDetail: DStream[SaleDetail] = noUserSaleDetialDStream.mapPartitions(iter => {

      //获取redis连接，用户数据 存放在了redis
      val jedisClient = RedisUtil.getJedisClient
      //2.查询Redis补充信息
      val details: Iterator[SaleDetail] = iter.map(saleDetail => {
        //得到userinfo
        //redis区分大小写，注意区分，然后就是出现bug的时候可以一步一步print打印看一下
        val userRedisKey = s"UserInfo:${saleDetail.user_id}"
        //判断redis里是否存在userinfo，如果不存在就去mysql中找
        if (jedisClient.exists(userRedisKey)) {
          //取出userinfo
          val userJson: String = jedisClient.get(userRedisKey)

          //将用户数据转化为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userJson,classOf[UserInfo])

          //返回补全信息
          saleDetail.mergeUserInfo(userInfo)
        }else {
          //获取mysql连接
          val connection: Connection = JdbcUtil.getConnection

          //根据UserId查询到数据，访问Mysql用户信息
          val userStr = JdbcUtil.getUserInfoFromMysql(connection,
            "select * from user_info where id=?",
            Array(saleDetail.user_id))

          //将用户信息转化为样例类
          val userInfo = JSON.parseObject(userStr,classOf[UserInfo])

          //返回补全信息
          saleDetail.mergeUserInfo(userInfo)

          //关闭连接
          connection.close()
        }

        //返回数据
        saleDetail
      })

      //归还连接
      jedisClient.close()

      //返回数据
      details
    })

    //8.将三张表join的结果写入ES
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    saleDetail.foreachRDD( rdd => {
      rdd.foreachPartition( iter => {
        //IndexName
        val date: String = sdf.format(new Date(System.currentTimeMillis()))
        val indexName = s"${GmallConstant.ES_SALE_DETAIL_INDEX_PRE}-${date}"

        //准备数据集
        val detailIdToSaleDetail: List[(String, SaleDetail)] = iter.toList.map(saleDetail => (saleDetail.order_detail_id,saleDetail))

        //执行批量写入
        MyEsUtil.insertBulk(indexName,detailIdToSaleDetail)
      })
    })

    /*
    //测试
    //saleDetail.print(100)

    //测试
    noUserSaleDetialDStream.print()
    //测试
    orderInfoKafkaDStream.foreachRDD(
      rdd => {
        rdd.foreach( record => println(record.value()))
      })

    orderDetailKafkaDStream.foreachRDD(
      rdd => {
        rdd.foreach( record => println(record.value()))
      })*/


    //开启
    ssc.start()
    ssc.awaitTermination()
  }

}
