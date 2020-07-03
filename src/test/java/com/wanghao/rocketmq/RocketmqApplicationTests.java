package com.wanghao.rocketmq;

import com.wanghao.rocketmq.utils.SpringProducer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
class RocketmqApplicationTests {

    @Autowired
    private SpringProducer producer;

    @Value("${rocketmq.topic}")
    private String topic;

    @Test
    void contextLoads() throws Exception {
        DefaultMQProducer producer = this.producer.getProducer();
        for (int i = 0; i < 10; i++) {
            Message message = new Message(topic, null, (" Hello Java demo RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(message);
        }
    }

}
