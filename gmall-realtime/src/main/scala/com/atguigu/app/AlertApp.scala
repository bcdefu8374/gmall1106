package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstant
import com.atguigu.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._


/**
 * @author chen
 * @topic
 * @create 2020-11-10
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //2.创建streamingcontext
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //3.从kafka获取数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_EVENT,ssc)


    //4.将每行数据转化为样例类，补充时间字段，并将数据转化为kv结构(mid,log)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream = kafkaDStream.map(record => {
      //1.转化为样例类
      val eventlog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      //2.处理时间字段
      val dateHourStr: String = sdf.format(new Date(eventlog.ts))
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      eventlog.logDate = dateHourArr(0)
      eventlog.logHour = dateHourArr(1)

      //返回数据
      (eventlog.mid, eventlog)
    })

    //5.开窗5min
    val midToLogByWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.按照mid分组
    val midToLogGroup: DStream[(String, Iterable[EventLog])] = midToLogByWindowDStream.groupByKey()

    //7.组内筛选数据
    val boolToAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogGroup.map {
      case (mid, iter) =>

        //1.创建Set用于存放领劵的uid
        val uids: util.HashSet[String] = new util.HashSet[String]()

        //2.创建set用于存放优惠劵涉及的商品ID
        val itemIds = new util.HashSet[String]()

        //3.创建List用于存放发生过的所有行为
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //4.定义标志位，用于记录是否存在浏览商品行为
        var noClick: Boolean = true

        //5.遍历iter
        breakable {
          iter.foreach(log => {
            val evid: String = log.evid
            events.add(evid)

            //判断是否为浏览商品行为
            if ("clickItem".equals(evid)) {
              noClick = false
              break()
            } else if ("coupon".equals(evid)) {
              uids.add(log.uid)
              itemIds.add(log.itemid)
            }
          })
        }

        (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events,System.currentTimeMillis()))
        //(uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }
    //8.生成预警日志
    val alterInfoDStream = boolToAlertInfoDStream.filter(_._1).map(_._2)
    alterInfoDStream.print()

    //9.写入ES
    alterInfoDStream.foreachRDD( rdd => {
      rdd.foreachPartition( iter => {
        //创建索引名
        val toydayStr: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        val indexName = s"${GmallConstant.ES_ALERT_INDEX_PRE}-$toydayStr"

        //处理数据，补充docId
        val docList = iter.toList.map(alterInfo => {
          val min: Long = alterInfo.ts / 1000 / 60
          (s"${alterInfo.mid}-$min", alterInfo)
        })

        //执行批量写入操作
        MyEsUtil.insertBulk(indexName,docList)

      })
    })


    //10.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
