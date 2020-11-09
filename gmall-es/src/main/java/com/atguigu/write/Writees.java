package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author chen
 * @topic
 * @create 2020-11-09
 */
public class Writees {
    public static void main(String[] args) throws IOException {
        //1.创建ES的客户端构造器
        JestClientFactory clientFactory = new JestClientFactory();

        //2.创建ES客户端的连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置ES的连接地址
        clientFactory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = clientFactory.getObject();

        //5.构建ES插入数据对象
        Movie movie = new Movie("1003", "金刚川");
        Index index = new Index.Builder(movie)
                .index("movie_test2")
                .type("_doc")
                .id("1003")
                .build();

        //6.执行插入数据操作
        jestClient.execute(index);

        //7.关闭连接
        jestClient.shutdownClient();
    }
}
