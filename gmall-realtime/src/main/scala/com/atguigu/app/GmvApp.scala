package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.util.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.StreamingContext
import org.apache.phoenix.spark._

/**
 * @author chen
 * @topic
 * @create 2020-11-08
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //初始化配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,streaming.Seconds(5))

    //3.消费kafka数据
    val kafkanDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO,ssc)

    //将每行数据转化为样例类对象，补充时间，数据脱敏（手机号）
    val orderInfoDStream: DStream[OrderInfo] = kafkanDStream.map(record => {

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
      orderinfo
    })

    //Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS",
    // "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
    // "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO",
    // "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR")
    //5.写入phoenix
    orderInfoDStream.foreachRDD(
      rdd => {
        //加一行注释测试
        println("aaaaaaaaaaaaaaa")
        rdd.saveToPhoenix(
          "GMALL2020_ORDER_INFO",
          classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    )


    //开启和阻塞
    ssc.start()
    ssc.awaitTermination()
  }

}
