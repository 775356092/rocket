package com.wanghao.rocketmq.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

/**
 * @description: 消息生产者
 * @author: wang hao
 * @create: 2020-07-03 15:26
 */
public class Consumer {
    public static void main(String[] args) throws Exception {
        // 创建一个消息消费者，并设置一个消息消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer_group");

        // 指定NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");

        // 设置Consumer第一次启动时是从队列头部还是尾部开始消费的
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 订阅指定Topic下的所有消息
        consumer.subscribe("test_topic", "*");

        // 注册消息监听器
        consumer.registerMessageListener((List<MessageExt> list, ConsumeConcurrentlyContext context) -> {
            // 默认list里只有一条消息，可以通过设置参数来批量接收消息
            if (list != null) {
                for (MessageExt ext : list) {
                    try {
                        System.out.println(new Date() + new String(ext.getBody(), "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 消费者对象在使用之前必须要调用start方法初始化
        consumer.start();
        System.out.println("消息消费者已启动");
    }
}
