package com.alibaba.middleware.race.util;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.rocketmq.ConsumerFactory;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

public class ClearRocketMQ {
    
    private DefaultMQPushConsumer _allMsgConsumer = null;
    
    AtomicInteger recieveTupleCount = new AtomicInteger(0);

    public static void main(String[] args) throws MQClientException, InterruptedException {
        ClearRocketMQ testConsumerGroup = new ClearRocketMQ();
        testConsumerGroup.setupMessageConsumer();
        System.out.println("----------------------");
    }
    
    /**
     * @throws MQClientException 
     * @throws InterruptedException 
     * 
     */
    private void setupMessageConsumer() throws InterruptedException, MQClientException {
        _allMsgConsumer = ConsumerFactory.create(RaceConfig.MetaConsumerGroup,
                RaceConfig.MqTmallTradeTopic, RaceConfig.MqPayTopic, RaceConfig.MqTaobaoTradeTopic);
        _allMsgConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for(MessageExt msg : msgs) {
                    System.out.println(recieveTupleCount.addAndGet(1));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        _allMsgConsumer.start();
    }
}
