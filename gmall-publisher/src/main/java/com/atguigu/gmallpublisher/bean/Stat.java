package com.atguigu.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author chen
 * @topic
 * @create 2020-11-12
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Stat {
    private String title;
    private List<Option> options;

}
