package com.wanghao.rocketmq.test;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;

/**
 * @description: 消息生产者
 * @author: wang hao
 * @create: 2020-07-03 15:26
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        // 创建一个消息生产者，并设置一个消息生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test_producer_group");

        // 指定NameServer 地址
        producer.setNamesrvAddr("localhost:9876");

        // 初始化Producer，在整个应用生命周期中只需要初始化一次
        producer.start();

        for (int i = 0; i < 100; i++) {
            // 创建一个消息对象，指定其主题、标签和消息内容
            Message message = new Message("test_topic", "TagA", ("Hello Java demo RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            // 发送消息并返回结果
            SendResult sendResult = producer.send(message);
            System.out.printf("%s%n", sendResult);
        }

        // 一旦生产者实例不再被使用，则将其关闭，包括清理资源、关闭网络连接等
        producer.shutdown();
    }
}
