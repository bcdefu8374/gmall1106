package com.atguigu.util;

import java.util.Random;

/**
 * @author chen
 * @topic
 * @create 2020-11-06
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return fromNum + new Random().nextInt(toNum-fromNum+1);
    }
}

