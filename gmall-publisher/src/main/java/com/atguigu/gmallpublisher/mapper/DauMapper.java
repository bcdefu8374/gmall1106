package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author chen
 * @topic
 * @create 2020-11-06
 */

public interface DauMapper {

    //将来执行这个接口方法时候实际执行的的是select
    //总的统计
    public Integer selectDauTotal(String date);


    //分时统计
    public List<Map> selectDauTotalHourMap(String date);
}
