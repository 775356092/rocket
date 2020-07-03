package com.wanghao.rocketmq.utils;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
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
public class SpringProducer {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${rocketmq.producerGroup}")
    private String producerGroup;

    @Value("${rocketmq.namesrvAddr}")
    private String namesrvAddr;

    private DefaultMQProducer producer;

    @PostConstruct
    public void init() throws Exception {
        logger.info("开始启动消息生产者服务...");
        // 创建一个消息生产者，并设置一个消息生产者组
        producer = new DefaultMQProducer(producerGroup);
        // 指定NameServer地址
        producer.setNamesrvAddr(namesrvAddr);
        // 初始化SpringProducer，在整个应用生命周期内只需要初始化一次、
        producer.start();
    }

    public void destroy() {
        logger.info("开始关闭消息生产者服务...");
        producer.shutdown();
        logger.info("消息生产者服务已关闭.");
    }

    public DefaultMQProducer getProducer(){
        return producer;
    }

}
