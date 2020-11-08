package com.atguigu.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author chen
 * @topic
 * @create 2020-11-06
 */
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}
