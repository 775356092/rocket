package com.wanghao.rocketmq.utils;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @description:
 * @author: wang hao
 * @create: 2020-07-03 16:38
 */
@Component
public class SpringConsumer {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${rocketmq.consumerGroup}")
    private String consumerGroup;

    @Value("${rocketmq.namesrvAddr}")
    private String namesrvAddr;

    @Value("${rocketmq.topic}")
    private String topic;

    private DefaultMQPushConsumer consumer;

    @PostConstruct
    public void init() throws Exception {
        logger.info("开始启动消息消费者服务...");
        // 创建一个消息消费者，并设置一个消息消费者组
        consumer = new DefaultMQPushConsumer(consumerGroup);
        // 指定NameServer地址
        consumer.setNamesrvAddr(namesrvAddr);
        // 设置Consumer第一次启动时是从队列头部还是队列尾部开始消费的
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅指定Topic下的所有消息
        consumer.subscribe(topic, "*");
        // 注册消息监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
            try {
                if (list != null) {
                    for (MessageExt messageExt : list) {
                        logger.info("监听到消息: " + new String(messageExt.getBody()));//输出消息内容
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER; //稍后再试
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //消费成功
        });
        // 消费者对象在使用之前必须要掉要start方法初始化
        consumer.start();
        logger.info("消息消费者启动成功.");
    }

    public void destroy() {
        logger.info("开始关闭消息消费者服务...");
        consumer.shutdown();
        logger.info("消息消费者服务已关闭.");
    }



}
