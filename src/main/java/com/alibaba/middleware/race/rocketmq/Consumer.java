package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;

import java.util.List;

public class Consumer {
	private static int counter = 0;

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        
//        consumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
        System.out.println(consumer.getNamesrvAddr());

        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
        consumer.subscribe(RaceConfig.MqPayTopic, "*");
        
        
        consumer.setPullBatchSize(1000);

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {

                    byte [] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        System.out.println("Got the end signal");
                        continue;
                    }

                    if(msg.getTopic().equals(RaceConfig.MqPayTopic)){
                    	PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                        counter++;
                        System.out.println("" + counter + paymentMessage);
                    }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
                    	OrderMessage orderMessage= RaceUtils.readKryoObject(OrderMessage.class, body);
                        counter++;
                        System.out.println("" + counter + orderMessage);
                    }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
                    	 OrderMessage orderMessage= RaceUtils.readKryoObject(OrderMessage.class, body);
                         counter++;
                         System.out.println("" + counter + orderMessage);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer Started.");
    }
}
