//package com.alibaba.middleware.unused;
//
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.jstorm.utils.JStormUtils;
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.RaceUtils;
//import com.alibaba.middleware.race.jstorm.RaceTopology;
//import com.alibaba.middleware.race.model.OrderMessage;
//import com.alibaba.middleware.race.model.PaymentMessage;
//import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
//import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
//import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
//import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
//import com.alibaba.rocketmq.client.exception.MQClientException;
//import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
//import com.alibaba.rocketmq.common.message.MessageExt;
//
//import backtype.storm.spout.SpoutOutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.IRichSpout;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Values;
//
//public class NewAllSpout implements IRichSpout {
//
//	private static final long serialVersionUID = -8949381451255846180L;
//	
//	private static Logger LOG = LoggerFactory.getLogger(NewAllSpout.class);
//	private SpoutOutputCollector _collector;
//
//	private int _sendNumPerNexttuple = RaceConfig.DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE;
//	private AtomicInteger DEBUG_receivedPaymentMsgCount = new AtomicInteger(0);//TODO just for debug
//    private AtomicInteger DEBUG_amountEqualsZeroPaymentMsgCount = new AtomicInteger(0);
//    private long DEBUG_sendPaymentCount = 0;
//    private long DEBUG_sendTMTradeeCount = 0;
//    private long DEBUG_sendTBTradeCount = 0;
//    private long DEBUG_sendFailedCount = 0;
//	
//    private AtomicBoolean _paymentMsgEndSignal = new AtomicBoolean(false);
//    private AtomicLong _latestMsgArrivedTime = new AtomicLong(0);
//    private static final long CONSUMER_MAX_WAITING_TIME = 1 * 60 * 1000;//此时间内收不到任何消息，且_paymentMsgEndSignal为true,则认为所有消息接收完成
//    
//    private transient LinkedBlockingQueue<PaymentMessage> payMessageQueue;    
//    private transient LinkedBlockingQueue<OrderMessage> TMTradeMessage;
//    private transient LinkedBlockingQueue<OrderMessage> TBTradeMessage;
//	
//    private void initPayConsumer() throws MQClientException{
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
//        
//        payMessageQueue = new LinkedBlockingQueue<PaymentMessage>();        
//        TMTradeMessage = new LinkedBlockingQueue<OrderMessage>();       
//        TBTradeMessage = new LinkedBlockingQueue<OrderMessage>();  
//        
//        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
//        consumer.subscribe(RaceConfig.MqPayTopic, "*");
//        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
//        
//        consumer.setPullBatchSize(RaceConfig.MQBatchSize);
////      payConsumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
//        
//        consumer.registerMessageListener(new MessageListenerConcurrently() {
//
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
//                                                            ConsumeConcurrentlyContext context) {
//                _latestMsgArrivedTime.set(System.currentTimeMillis());
//                for (MessageExt msg : msgs) {
//                     byte [] body = msg.getBody();
//                     if (body.length == 2 && body[0] == 0 && body[1] == 0) {
//                         if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
//                             _paymentMsgEndSignal.set(true);
//                         }
//                         System.out.println("Got the end signal");
//                         continue;
//                     }
//                     
//                     if(msg.getTopic().equals(RaceConfig.MqPayTopic)){
//                         DEBUG_receivedPaymentMsgCount.addAndGet(1);
//                         PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
//                         payMessageQueue.offer(paymentMessage);
//                     }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
//                         OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//                         TBTradeMessage.offer(orderMessage);
//                     }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
//                         OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//                         TMTradeMessage.offer(orderMessage);
//                     }
//                 }
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            }
//        });
//
//        consumer.start();
//    }
//    
//    @Override
//	public void ack(Object arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void activate() {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void close() {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void deactivate() {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void fail(Object m) {
//		// TODO Auto-generated method stub
//		MsgID msgID = (MsgID) m;
//		_collector.emit(msgID.streamID, msgID.values);
//		++DEBUG_sendFailedCount;
//
//	}
//
//	@Override
//	public void nextTuple() {
//        // TODO Auto-generated method stub
//		
//        for (int i = 0; i < _sendNumPerNexttuple; ++i) {
//            if(!payMessageQueue.isEmpty()){
//                try {
//                    PaymentMessage paymentMessage = payMessageQueue.take();
//                    Values values = new Values(paymentMessage.getOrderId(), paymentMessage);
//                    _collector.emit(RaceTopology.PAYMENTSTREAM, values, new MsgID(RaceTopology.PAYMENTSTREAM, values));
//                    LOG.info("New AllSpout emit Paymessage" + values);
//                    DEBUG_sendPaymentCount++;
//
//                } catch (InterruptedException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//            
//            if(!TMTradeMessage.isEmpty()){
//            	try {
//                    OrderMessage orderMessage = TMTradeMessage.take();
//                    Values values = new Values(orderMessage.getOrderId(), orderMessage);
//                    _collector.emit(RaceTopology.TMTRADESTREAM, values, new MsgID(RaceTopology.TMTRADESTREAM, values));
//                    LOG.info("New AllSpout emit TMmessage" + values);
//                    DEBUG_sendTMTradeeCount++;
//
//                } catch (InterruptedException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//            
//            if(!TBTradeMessage.isEmpty()){
//            	try {
//                    OrderMessage orderMessage = TBTradeMessage.take();
//                    Values values = new Values(orderMessage.getOrderId(), orderMessage);
//                    _collector.emit(RaceTopology.TBTRADESTREAM, values, new MsgID(RaceTopology.TBTRADESTREAM, values));
//                    LOG.info("New AllSpout emit TBmessage" + values);
//                    DEBUG_sendTBTradeCount++;
//
//                } catch (InterruptedException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//       }
//        
//        long current = System.currentTimeMillis();
//        if (payMessageQueue.isEmpty()
//                && TMTradeMessage.isEmpty()
//                && TBTradeMessage.isEmpty()
//                && current - _latestMsgArrivedTime.get() > CONSUMER_MAX_WAITING_TIME) {
////            sendEndSignals();
//            logDebugInfo();
//            JStormUtils.sleepMs(2000);
//        }
//        
//    
//
//	}
//
//	@Override
//	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//		_collector = collector;
//        _sendNumPerNexttuple = JStormUtils.parseInt(
//                conf.get("send.num.each.time"), RaceConfig.DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE);
//        
//        try {
//            initPayConsumer();
//        } catch (MQClientException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }       
//
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declare) {
//		// TODO Auto-generated method stub
//        declare.declareStream(RaceTopology.PAYMENTSTREAM, new Fields("orderID", "paymentMessage"));
//        declare.declareStream(RaceTopology.TMTRADESTREAM, new Fields("orderID", "TMTradeMessage"));
//        declare.declareStream(RaceTopology.TBTRADESTREAM, new Fields("orderID", "TBTradeMessage"));
//	}
//
//	@Override
//	public Map<String, Object> getComponentConfiguration() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//	
//	public void logDebugInfo() {
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_payMessageCount:{}", DEBUG_receivedPaymentMsgCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_amountEqualsZeroPaymentMsgCount:{}", DEBUG_amountEqualsZeroPaymentMsgCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_sendPaymentCount:{}", DEBUG_sendPaymentCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_sendTBTradeCount:{}", DEBUG_sendTBTradeCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_sendTMTradeeCount:{}", DEBUG_sendTMTradeeCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_sendFailedCount:{}", DEBUG_sendFailedCount);
//    }
//	
//	private class MsgID{
//		String streamID;
//		Values values;
//		
//		public MsgID(String streamID, Values values){
//			this.streamID = streamID;
//			this.values = values;
//		}
//	}
//
//}
