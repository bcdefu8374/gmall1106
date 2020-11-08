package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstant;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author chen
 * @topic
 * @create 2020-11-07
 * 客户端是用来抓取Canal里面监控到的数据，然后把数据做解析之后写到kafka里面
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {

        //1.获取Canal连接对象,这个是客户端和canal的连接的，不是连接mysql的，得到canal的连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");


        //2.抓取连接数据并解析，一直要抓取，所以相当于一个死循环
        while (true) {

            //1.连接canal和mysql的连接
            canalConnector.connect();

            //2.指定消费的数据mysql中表，订阅主题的感觉
            canalConnector.subscribe("gmall2020.*");

            //3.抓取数据 从mysql的log文件一次拉取100行，有多少拿多少，如果低于100，有多少拿多少，
            // 如果高于100就100,100拿
            Message message = canalConnector.get(100);

            //4.判空,判断entry是否是否为空，说明，当前这一次没有抓到数据，mysql里面当前没有人在更新数据，
            // 紧接着再去抓，很可能还是空的，所以就sleep等待一下，不要频繁的去抓取
            if (message.getEntries().size() <= 0){
                System.out.println("当前没有数据休息一下！");
                try {
                    Thread.sleep(5000);
                }catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {

                //能走到这里，说明一定不为空
                //5.获取entry集合
                List<CanalEntry.Entry> entries = message.getEntries();

                //6.遍历Entry
                for (CanalEntry.Entry entry : entries) {
                    //7.获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //8.判断，只去RowData类型
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){

                        //1.获取表名
                        String tableName = entry.getHeader().getTableName();

                        //2.获取序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        //3.反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //4.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //5.获取数据集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //6.根据表名以及事件类型处理数据rowDatasList
                        handler(tableName,eventType,rowDatasList);
                    }
                }

            }


        }


    }


    /**
     * 根据表名以及处理事件类型处理数据rowDatasList
     * @param tableName 表名
     * @param eventType 事件类型
     * @param rowDatasList 数据集合
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        //求的是GMV，订单表，新增及变化 ，对于订单表而言，用的是新增和变化，新增一定要，变化的就不要了
        //对于订单表而言就只要新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {

                //创建json对象，用于存放多个列的数据
                JSONObject jsonObject = new JSONObject();

                //对于更新数据而言，我们只要修改后的数据
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(),column.getValue());
                }

                //打印
                System.out.println(jsonObject.toString());

                //发送数据到 kafka
                MyKafkaSender.send(GmallConstant.GMALL_ORDER_INFO,jsonObject.toString());


            }
        }
    }

}
