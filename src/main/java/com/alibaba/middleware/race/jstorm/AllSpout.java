package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.MetaMessage;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.ConsumerFactory;
import com.alibaba.middleware.race.util.FileUtil;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class AllSpout implements IRichSpout, MessageListenerConcurrently {

	private static final long serialVersionUID = -8949381451255846180L;
	
	private static Logger LOG = LoggerFactory.getLogger(AllSpout.class);
	private SpoutOutputCollector _collector;

    public static long DEBUG_spoutStartTime;
//     private static final boolean DEBUG_ENABLE = true;
//     private String DEBUG_thisSpoutName;
//     private AtomicInteger DEBUG_receivedMsgCount = new AtomicInteger(0);
//     private AtomicInteger DEBUG_amountEqualsZeroPaymentMsgCount = new AtomicInteger(0);
//     private AtomicInteger DEBUG_sendTupleCount = new AtomicInteger(0);
//     private AtomicInteger DEBUG_resendCount = new AtomicInteger(0);

	
    private AtomicBoolean _paymentMsgEndSignal = new AtomicBoolean(false);
    private AtomicLong _latestMsgArrivedTime = new AtomicLong(0);

    private transient LinkedBlockingQueue<MetaMessage> sendingQueue;
	
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        DEBUG_spoutStartTime = System.currentTimeMillis();
//        if (DEBUG_ENABLE) {
//            DEBUG_thisSpoutName = Thread.currentThread().getName();
//        }
        _collector = collector;
        sendingQueue = new LinkedBlockingQueue<MetaMessage>();
        initConsumer();
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderID", "metaMessage"));
    }
    
    @Override
    public void fail(Object msgId) {
//        _collector.emit(new Values(msgId), msgId);//TODO
//        DEBUG_resendCount.addAndGet(1);
    }

    private void initConsumer() {
        try {
            DefaultMQPushConsumer consumer = ConsumerFactory.create(
                    this,
                    RaceConfig.MetaConsumerGroup,
                    RaceConfig.MqTmallTradeTopic, 
                    RaceConfig.MqPayTopic,
                    RaceConfig.MqTaobaoTradeTopic);
//            if (DEBUG_ENABLE) {
//              if(consumer == null){
//                  FileUtil.appendLineToFile(DEBUG_FILES_OUTPUT_DIR + "consumer.txt", DEBUG_thisSpoutName + "Consumer already exist consumer in current worker, don't need to fetch data!");
//              } else {
//                  FileUtil.appendLineToFile(DEBUG_FILES_OUTPUT_DIR + "consumer.txt", DEBUG_thisSpoutName + "create consumer successfully!");
//              }
//            }
        } catch (MQClientException e) {
            e.printStackTrace();
            LOG.error("Failed in initConsumer", e);
            throw new RuntimeException("Failed in initConsumer", e);
        }
    }

    @Override
    public void nextTuple() {
        if (!sendingQueue.isEmpty()) {
            for (int i = 0; i < Constants.SPOUT_MAX_SEND_NUMBER_PER_NEXT_TUPLE
                    && !sendingQueue.isEmpty(); ++i) {
                MetaMessage metaTuple = sendingQueue.poll();
                if (metaTuple != null) {
                    _collector.emit(new Values(metaTuple.getOrderId(),
                            metaTuple));
//                    if (DEBUG_ENABLE) {
//                        int tmpCount = DEBUG_sendTupleCount.addAndGet(1);
//                        FileUtil.appendLineToFile(
//                                Constants.DEBUG_FILES_OUTPUT_DIR + "send.txt",
//                                DEBUG_thisSpoutName + ":DEBUG_sendTupleCount "
//                                        + tmpCount);
//                        FileUtil.appendLineToFile(
//                                Constants.DEBUG_FILES_OUTPUT_DIR
//                                        + "detail_tuples.txt",
//                                DEBUG_thisSpoutName + " : "
//                                        + metaTuple.toString());
//                        FileUtil.appendLineToFile(
//                                Constants.DEBUG_FILES_OUTPUT_DIR + "tuples.txt",
//                                metaTuple.toString());
//                    }
                }
            }
        } else {
            if (isConsumerTimeOut()) {
                if (_paymentMsgEndSignal.get()) {
                    // no more message coming for sure
                    // sendEndSignals();
                    // logDebugInfo();
                    JStormUtils.sleepMs(Constants.NO_MORE_MESSAGE_SLEEP_TIME);
                } else {
                    // there maybe more message coming (low probability)
                    JStormUtils.sleepMs(Constants.CONSUMER_TIMEOUT_SLEEP_TIME);
                }
            } else {
                // there maybe more message coming (high probability)
                JStormUtils.sleepMs(Constants.EMPTY_SEND_QUEUE_SLEEP_TIME);
            }
        }
    }
    
    public boolean isConsumerTimeOut() {
        long current = System.currentTimeMillis();
        return current - _latestMsgArrivedTime.get() > Constants.SPOUT_CONSUMER_MAX_WAITING_TIME;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) {
    	
        try {
            if (msgs != null && msgs.size() > 0) {
                _latestMsgArrivedTime.set(System.currentTimeMillis());
                String topic = context.getMessageQueue().getTopic();
                for (MessageExt msg : msgs) {
                	String msgID = msg.getMsgId();
                	
                    byte[] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
                            _paymentMsgEndSignal.set(true);
                        }
                        LOG.info("Got the end signal");
                        continue;
                    }

                    if (RaceConfig.MqPayTopic.equals(topic)) {
//                        if (DEBUG_ENABLE) {
//                          int tmpCount = DEBUG_receivedMsgCount.addAndGet(1);
//                          FileUtil.appendLineToFile(Constants.DEBUG_FILES_OUTPUT_DIR + "receive.txt", DEBUG_thisSpoutName + ":DEBUG_receivedMsgCount " + tmpCount);
//                        }

                        PaymentMessage paymentMessage = RaceUtils
                                .readKryoObject(PaymentMessage.class, body);
                        if (paymentMessage.getPayAmount() > Constants.ZERO_THREHOLD) {
                            sendingQueue.offer(new MetaMessage(paymentMessage,
                                    topic, msgID));
                        } else {
//                            if (DEBUG_ENABLE) {
//                                int tmpCount2 = DEBUG_amountEqualsZeroPaymentMsgCount.addAndGet(1);
//                                FileUtil.appendLineToFile(Constants.DEBUG_FILES_OUTPUT_DIR + "zero.txt", DEBUG_thisSpoutName + ":DEBUG_amountEqualsZeroPaymentMsgCount " + tmpCount2);
//                            }
                        }
                    } else if (RaceConfig.MqTmallTradeTopic.equals(topic)
                            || RaceConfig.MqTaobaoTradeTopic.equals(topic)) {
//                        if (DEBUG_ENABLE) {
//                          int tmpCount = DEBUG_receivedMsgCount.addAndGet(1);
//                          FileUtil.appendLineToFile(Constants.DEBUG_FILES_OUTPUT_DIR + "receive.txt", DEBUG_thisSpoutName + ":DEBUG_receivedMsgCount " + tmpCount);
//                        }
                        OrderMessage orderMessage = RaceUtils.readKryoObject(
                                OrderMessage.class, body);
                        sendingQueue
                                .offer(new MetaMessage(orderMessage, topic, msgID));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed in consumeMessage.", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    
    @Override
    public void ack(Object arg0) {
    }

    @Override
    public void activate() {
    }

    @Override
    public void close() {
    }

    @Override
    public void deactivate() {

    }

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

    public void logDebugInfo() {
/*
        LOG.info("[{}.logDebugInfo] DEBUG_receivedMsgCount:{}",
                DEBUG_thisSpoutName, DEBUG_receivedMsgCount);
        LOG.info("[{}.logDebugInfo] DEBUG_amountEqualsZeroPaymentMsgCount:{}",
                DEBUG_thisSpoutName, DEBUG_amountEqualsZeroPaymentMsgCount);
        LOG.info("[{}.logDebugInfo] DEBUG_sendTupleCount:{}",
                DEBUG_thisSpoutName, DEBUG_sendTupleCount);
        LOG.info("[{}.logDebugInfo] DEBUG_resendCount:{}",
                DEBUG_thisSpoutName, DEBUG_resendCount);
*/
    }
}
