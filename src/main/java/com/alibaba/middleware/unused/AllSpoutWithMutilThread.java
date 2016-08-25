//package com.alibaba.middleware.unused;
//
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.jstorm.utils.JStormUtils;
//import com.alibaba.middleware.race.Constants;
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.RaceUtils;
//import com.alibaba.middleware.race.model.PaymentMessageExt;
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
//public class AllSpoutWithMutilThread implements IRichSpout, Runnable{
//	private static final long serialVersionUID = 282914905327080472L;
//
//	private static Logger LOG = LoggerFactory.getLogger(AllSpoutWithMutilThread.class);
//    private SpoutOutputCollector _collector;
//    
//    private long TMLastTime = 0;
//    private long TBLastTime = 0;
//    private int _sendNumPerNexttuple = RaceConfig.DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE;
//
//    private AtomicInteger DEBUG_receivedPaymentMsgCount = new AtomicInteger(0);//TODO just for debug
//    private AtomicInteger DEBUG_amountEqualsZeroPaymentMsgCount = new AtomicInteger(0);
//    private long DEBUG_sendTupleNormallyCount = 0;
//    private long DEBUG_solveFailedCount = 0;
//    private long DEBUG_sendEmptyTupleCount = 0;
//    private long DEBUG_failedTupleCount = 0;
//
//    private AtomicBoolean _paymentMsgEndSignal = new AtomicBoolean(false);
//    private AtomicLong _latestMsgArrivedTime = new AtomicLong(0);
//    private static final long CONSUMER_MAX_WAITING_TIME = 1 * 60 * 1000;//此时间内收不到任何消息，且_paymentMsgEndSignal为true,则认为所有消息接收完成
//    private boolean _isRunning = true;
//    
//    private transient LinkedBlockingQueue<PaymentMessageExt> solvedPayMessageQueue;//已经有salerPlatform的信息
//    private transient LinkedBlockingQueue<PaymentMessageExt> unSolvedPayMessageQueue;//还没有salerPlatform的信息
//    
//    private transient ConcurrentHashMap<Long, Double> TMTradeMessage;
//    private transient ConcurrentHashMap<Long, Double> TBTradeMessage;
//    
//    private void initPayConsumer() throws MQClientException{
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
//        
//        this.solvedPayMessageQueue = new LinkedBlockingQueue<PaymentMessageExt>();
//        this.unSolvedPayMessageQueue = new LinkedBlockingQueue<PaymentMessageExt>();
//        
//        TMTradeMessage = new ConcurrentHashMap<Long, Double>(RaceConfig.MapInitCapacity);      
//        
//        TBTradeMessage = new ConcurrentHashMap<Long, Double>(RaceConfig.MapInitCapacity);
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
//                         if (paymentMessage.getPayAmount() > 0.0) {
//                             PaymentMessageExt paymentMessageExt = new PaymentMessageExt(paymentMessage);
//                             if(!solvePaymentMessageExt(paymentMessageExt)) {
//                                 ++DEBUG_solveFailedCount;
//                             }
//                         } else {
//                             DEBUG_amountEqualsZeroPaymentMsgCount.addAndGet(1);
//                         }
//                     }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
//                         OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//                         TBTradeMessage.put(orderMessage.getOrderId(), orderMessage.getTotalPrice());
//                     }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
//                         OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//                         TMTradeMessage.put(orderMessage.getOrderId(), orderMessage.getTotalPrice());
//                     }
//                 }
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            }
//        });
//
//        consumer.start();
//    }
//
//    private boolean solvePaymentMessageExt(PaymentMessageExt paymentMessageExt) {
//        boolean ret = false;
//        try {
//            long orderId = paymentMessageExt.getOrderId();
//            if (TBTradeMessage.containsKey(orderId)){
//                paymentMessageExt.setSalerPlatform(Constants.TAOBAO);
//                solvedPayMessageQueue.put(paymentMessageExt);
//
//                //update order
//                Double lastAmount = TBTradeMessage.get(orderId);
//                if(lastAmount - paymentMessageExt.getPayAmount() < 1e-2){
//                    TBTradeMessage.remove(orderId);
//                }else{
//                    TBTradeMessage.put(orderId, lastAmount - paymentMessageExt.getPayAmount());
//                }
//
//                ret = true;
//            } else if (TMTradeMessage.containsKey(orderId)) {
//                paymentMessageExt.setSalerPlatform(Constants.TMALL);
//                solvedPayMessageQueue.put(paymentMessageExt);
//                
//                //update order
//                Double lastAmount = TMTradeMessage.get(orderId);
//                if(lastAmount - paymentMessageExt.getPayAmount() < 1e-2){
//                    TMTradeMessage.remove(orderId);
//                }else{
//                    TMTradeMessage.put(orderId, lastAmount - paymentMessageExt.getPayAmount());
//                }
//                
//                ret = true;
//            } else {
//                unSolvedPayMessageQueue.put(paymentMessageExt);
//                ret = false;
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        return ret;
//    }
//
//    @Override
//    public void ack(Object arg0) {
//        // TODO Auto-generated method stub
//        
//    }
//
//    @Override
//    public void activate() {
//        // TODO Auto-generated method stub
//        
//    }
//
//    @Override
//    public void close() {
//        // TODO Auto-generated method stub
//        
//    }
//
//    @Override
//    public void deactivate() {
//        // TODO Auto-generated method stub
//        
//    }
//
//    @Override
//    public void fail(Object paymentMessageExt) {
//        // TODO Auto-generated method stub
////       ++DEBUG_failedTupleCount;
////        resendFailedPaymentMessage((PaymentMessageExt)paymentMessageExt);
//    }
//    
//    private void resendFailedPaymentMessage(PaymentMessageExt failedPaymentMessageExt){
//        sendSolvedPayMentmessageExt(failedPaymentMessageExt);
//    }
//
//    private void sendSolvedPayMentmessageExt(PaymentMessageExt solvedPaymentMessageExt){
//        Values values = new Values(solvedPaymentMessageExt.getOrderId(), solvedPaymentMessageExt.getCreateTime(), solvedPaymentMessageExt.getPayAmount(),
//                solvedPaymentMessageExt.getPayPlatform());
//        if (solvedPaymentMessageExt.isSalerPlatformTB()) {
//            _collector.emit(OldRaceTopology.TBPAYSTREAM, values);
//            
//            TBLastTime = System.currentTimeMillis();
//            
//            LOG.info("AllSpout Emit TBPayment" + ":" + solvedPaymentMessageExt.toString());
//        } else {
//            _collector.emit(OldRaceTopology.TMPAYSTREAM, values);
//            
//            TMLastTime = System.currentTimeMillis();
//            
//            LOG.info("AllSpout Emit TMPayment" + ":" + solvedPaymentMessageExt.toString());
//        }
//    }
//
//    @Override
//    public void nextTuple() {
//        // TODO Auto-generated method stub
//        for (int i = 0; i < _sendNumPerNexttuple; ++i) {
//            if(!solvedPayMessageQueue.isEmpty()){
//                try {
//                    PaymentMessageExt paymentMessageExt = solvedPayMessageQueue.take();
//                    sendSolvedPayMentmessageExt(paymentMessageExt);
//                    ++DEBUG_sendTupleNormallyCount;
//
//                } catch (InterruptedException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//        }
//        
////        long current = System.currentTimeMillis();
////        if (solvedPayMessageQueue.isEmpty()
////                && unSolvedPayMessageQueue.isEmpty()
////                && _paymentMsgEndSignal.get()
////                && current - _latestMsgArrivedTime.get() > CONSUMER_MAX_WAITING_TIME) {
//////            sendEndSignals();
////            logDebugInfo();
////            JStormUtils.sleepMs(2000);
////        }
//        
//    }
//
//    @Override
//    public void open(Map conf, TopologyContext arg1, SpoutOutputCollector collector) {
//        // TODO Auto-generated method stub
//        _collector = collector;
//        _sendNumPerNexttuple = JStormUtils.parseInt(
//                conf.get("send.num.each.time"), RaceConfig.DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE);
//        
//        try {
//            initPayConsumer();
//            new Thread(this, "solvePaymentMessageExtThread").start();
//        } catch (MQClientException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }       
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declare) {
//        // TODO Auto-generated method stub
//        declare.declareStream(OldRaceTopology.TMPAYSTREAM, new Fields("orderID", "createTime", "payAmount", "platForm"));
//        declare.declareStream(OldRaceTopology.TBPAYSTREAM, new Fields("orderID", "createTime", "payAmount", "platForm"));
//    }
//
//    @Override
//    public Map<String, Object> getComponentConfiguration() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//    
//    public void logDebugInfo() {
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_receivedPaymentMsgCount:{}", DEBUG_receivedPaymentMsgCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_amountEqualsZeroPaymentMsgCount:{}", DEBUG_amountEqualsZeroPaymentMsgCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_sendTupleNormallyCount:{}", DEBUG_sendTupleNormallyCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_solveFailedCount:{}", DEBUG_solveFailedCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_sendEmptyTupleCount:{}", DEBUG_sendEmptyTupleCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_failedTupleCount:{}", DEBUG_failedTupleCount);
//    }
//
//    @Override
//    public void run() {
//        while (_isRunning) {
//            for (int i = 0; i < unSolvedPayMessageQueue.size(); ++i) {
//                PaymentMessageExt paymentMessageExt = unSolvedPayMessageQueue.poll();//TODO change to take
//                if (paymentMessageExt != null) {
//                    if(!solvePaymentMessageExt(paymentMessageExt)) {
//                        ++DEBUG_solveFailedCount;
//                        if (DEBUG_solveFailedCount > 2000000) {//TODO
//                            LOG.info("DEBUG_solveFailed" + ":" + paymentMessageExt.toString());
//                        }
//                    }
//                }
//            }
//            if (unSolvedPayMessageQueue.isEmpty()) {
//                JStormUtils.sleepMs(1);//TODO
////                JStormUtils.sleepMs(2000);//TODO
//                LOG.info("sleeping...");
//            }
//        }
//    }
////    private void sendEndSignals() {
////        LOG.info("sending end signals...");
////        PaymentMessageExt paymentMessageExt = new PaymentMessageExt(0, 0.0, (short)0, RaceConfig.PC, Constants.END_SIGNAL_CREATE_TIME);
////        paymentMessageExt.set_salerPlatform(Constants.TAOBAO);
////        sendSolvedPayMentmessageExt(paymentMessageExt);
////
////        paymentMessageExt = new PaymentMessageExt(0, 0.0, (short)0, RaceConfig.PC, Constants.END_SIGNAL_CREATE_TIME);
////        paymentMessageExt.set_salerPlatform(Constants.TMALL);
////        sendSolvedPayMentmessageExt(paymentMessageExt);
////    }
//}
