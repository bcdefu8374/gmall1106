package com.atguigu.gmallpublisher.service;

import java.util.Map;

/**
 * @author chen
 * @topic
 * @create 2020-11-06
 */

//接口
public interface PublisherService {
    public Integer getDauTotal(String date);

    public  Map getDauTotalHourMap(String date);

    //获取总数
    public Double getOrderAmount(String date);

    //获取分时
    public  Map getOrderAmountHour(String date);
}
