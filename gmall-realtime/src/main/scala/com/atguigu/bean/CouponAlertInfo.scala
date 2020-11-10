package com.atguigu.bean

/**
 * @author chen
 * @topic
 * @create 2020-11-10
 */
case class CouponAlertInfo(mid: String,
                           uids: java.util.HashSet[String],
                           itemIds: java.util.HashSet[String],
                           events: java.util.List[String],
                           ts: Long)
