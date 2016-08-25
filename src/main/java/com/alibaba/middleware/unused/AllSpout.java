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
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.RaceUtils;
//import com.alibaba.middleware.race.model.OrderMessage;
//import com.alibaba.middleware.race.model.PaymentMessage;
//import com.alibaba.middleware.race.rocketmq.CounterFactory;
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
//public class AllSpout implements IRichSpout{
//	private static final long serialVersionUID = 282914905327080472L;
//
//	private static Logger LOG = LoggerFactory.getLogger(AllSpout.class);
//	
//	private SpoutOutputCollector _collector;
//	
//	private int paymentCounter = 0;
//	
//	private long TMLastTime = 0;
//	private long TBLastTime = 0;
//	private int _sendNumPerNexttuple = RaceConfig.DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE;
//
//	private AtomicInteger DEBUG_receivedPaymentMsgCount = new AtomicInteger(0);//TODO just for debug
//	private AtomicInteger DEBUG_amountEqualsZeroPaymentMsgCount = new AtomicInteger(0);
//	private long DEBUG_sendTupleNormallyCount = 0;
//	private long DEBUG_sendUnsolvedTupleCount = 0;
//	private long DEBUG_sendEmptyTupleCount = 0;
//	private long DEBUG_failedTupleCount = 0;
//
//	private AtomicBoolean _paymentMsgEndSignal = new AtomicBoolean(false);
//	private AtomicLong _latestMsgArrivedTime = new AtomicLong(0);
//	private static final long CONSUMER_MAX_WAITING_TIME = 1 * 60 * 1000;//此时间内收不到任何消息，且_paymentMsgEndSignal为true,则认为所有消息接收完成
//	
//	private transient LinkedBlockingQueue<PaymentMessage> payMessageQueue;
//	private transient LinkedBlockingQueue<PaymentMessage> unSolvedMessage;
//	
//	private transient ConcurrentHashMap<Long, Double> TMTradeMessage;
//	private transient FixedsizeLinkedHashMap completeTMTrade;
//	
//	private transient ConcurrentHashMap<Long, Double> TBTradeMessage;
//	private transient FixedsizeLinkedHashMap completeTBTrade;
//	
//	private void initPayConsumer() throws MQClientException{
//		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
//		
//		this.payMessageQueue = new LinkedBlockingQueue<PaymentMessage>();
//		this.unSolvedMessage = new LinkedBlockingQueue<PaymentMessage>();
//		
//		TMTradeMessage = new ConcurrentHashMap<Long, Double>(RaceConfig.MapInitCapacity);
//	 	TMTradeMessage.put(RaceConfig.specialTMOrderID, 0.1);	 	
//	 	completeTMTrade = new FixedsizeLinkedHashMap(RaceConfig.MapInitCapacity);
//	 	
//	 	TBTradeMessage = new ConcurrentHashMap<Long, Double>(RaceConfig.MapInitCapacity);
//		TBTradeMessage.put(RaceConfig.specialTBOrderID, 0.1);
//		completeTBTrade = new FixedsizeLinkedHashMap(RaceConfig.MapInitCapacity);
//		
//		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//		consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
//		consumer.subscribe(RaceConfig.MqPayTopic, "*");
//		consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
//		
//		consumer.setPullBatchSize(RaceConfig.MQBatchSize);
////		payConsumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
//        
//        consumer.registerMessageListener(new MessageListenerConcurrently() {
//
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
//                                                            ConsumeConcurrentlyContext context) {
//                try {
//                    _latestMsgArrivedTime.set(System.currentTimeMillis());
//					for (MessageExt msg : msgs) {
//
//					     byte [] body = msg.getBody();
//					     if (body.length == 2 && body[0] == 0 && body[1] == 0) {
//					         if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
//					             _paymentMsgEndSignal.set(true);
//					         }
//					         System.out.println("Got the end signal");
//					         continue;
//					     }
//					     
//					     if(msg.getTopic().equals(RaceConfig.MqPayTopic)){
//					         DEBUG_receivedPaymentMsgCount.addAndGet(1);
//					    	 PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
//					    	 if (paymentMessage.getPayAmount() > 0.0) {
//					    	     payMessageQueue.put(paymentMessage);
//					    	 } else {
//					    	     DEBUG_amountEqualsZeroPaymentMsgCount.addAndGet(1);
//					    	 }
//					     }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
//					    	 OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//						     TBTradeMessage.put(orderMessage.getOrderId(), orderMessage.getTotalPrice());
//					     }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
//					    	 OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//						     TMTradeMessage.put(orderMessage.getOrderId(), orderMessage.getTotalPrice());
//					     }
//					 }
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            }
//        });
//
//        consumer.start();
//    }
//
//	
//	private void sendEmptyTMPayMessage(){
//		long TMOrderID = RaceConfig.specialTMOrderID;		
//		PaymentMessage paymentMessage = new PaymentMessage(TMOrderID, 0.0, (short)0, RaceConfig.PC, CounterFactory.timeStamp * 1000);
//		solvePayMentmessage(paymentMessage);
//		
//		paymentMessage = new PaymentMessage(TMOrderID, 0.0, (short)0, RaceConfig.Wireless, CounterFactory.timeStamp * 1000);
//		solvePayMentmessage(paymentMessage);
//	}
//	
//	private void sendEmptyTBPayMessage(){
//		long TBOrderID = RaceConfig.specialTBOrderID;
//		PaymentMessage paymentMessage = new PaymentMessage(TBOrderID, 0.0, (short)0, RaceConfig.PC, CounterFactory.timeStamp * 1000);
//		solvePayMentmessage(paymentMessage);
//		
//		paymentMessage = new PaymentMessage(TBOrderID, 0.0, (short)0, RaceConfig.Wireless, CounterFactory.timeStamp * 1000);
//		solvePayMentmessage(paymentMessage);
//	}
//
//	private void solvePayMentmessage(PaymentMessage paymentMessage){
//		paymentCounter++;
//		
//		Long orderID = paymentMessage.getOrderId();
//		
//		if(TMTradeMessage.containsKey(orderID)){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TMPAYSTREAM, values, paymentMessage);
//			
//			TMLastTime = System.currentTimeMillis();
//			
//			Double lastAmount = TMTradeMessage.get(orderID);
//			if(lastAmount - paymentMessage.getPayAmount() < 1e-6){
//				TMTradeMessage.remove(orderID);
//				completeTMTrade.put(orderID, 0.0);
//			}else{
//				TMTradeMessage.put(orderID, lastAmount - paymentMessage.getPayAmount());
//			}
//			
//			LOG.info("AllSpout Emit TMPayment" + paymentCounter + ":" + paymentMessage.toString());
//		}else if(TBTradeMessage.containsKey(orderID)){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TBPAYSTREAM, values, paymentMessage);
//			
//			TBLastTime = System.currentTimeMillis();			
//			Double lastAmount = TBTradeMessage.get(orderID);
//			if(lastAmount - paymentMessage.getPayAmount() < 1e-6){
//				TBTradeMessage.remove(orderID);
//				completeTBTrade.put(orderID, 0.0);
//			}else{
//				TBTradeMessage.put(orderID, lastAmount - paymentMessage.getPayAmount());
//			}
//			LOG.info("AllSpout Emit TBPayment" + paymentCounter + ":" + paymentMessage.toString());
//		}else{
//			unSolvedMessage.add(paymentMessage);
//		}
//	}
//	
//	private void solveFailPaymentMessage(PaymentMessage paymentMessage){
//		Long orderID = paymentMessage.getOrderId();
//		
//		if(TMTradeMessage.containsKey(orderID) || completeTMTrade.containsKey(orderID)){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TMPAYSTREAM, values, paymentMessage);
//			
//			TMLastTime = System.currentTimeMillis();			
//			LOG.info("AllSpout Emit TMPayment" + paymentCounter + ":" + paymentMessage.toString());
//		}else if(TBTradeMessage.containsKey(orderID) || completeTBTrade.containsKey(orderID)){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TBPAYSTREAM, values, paymentMessage);
//			
//			TBLastTime = System.currentTimeMillis();			
//			LOG.info("AllSpout Emit TBPayment" + paymentCounter + ":" + paymentMessage.toString());
//		}else{
//			unSolvedMessage.add(paymentMessage);
//		}
//	}
//	
//	@Override
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
//	public void fail(Object paymentMessage) {
//		// TODO Auto-generated method stub
//	    ++DEBUG_failedTupleCount;
//		solveFailPaymentMessage((PaymentMessage)paymentMessage);
//	}
//
//	@Override
//	public void nextTuple() {
//		// TODO Auto-generated method stub
//		
//	    for (int i = 0; i < _sendNumPerNexttuple; ++i) {
//	        if(!payMessageQueue.isEmpty()){
//	            try {
//	                PaymentMessage paymentMessage = payMessageQueue.take();
//	                solvePayMentmessage(paymentMessage);
//	                ++DEBUG_sendTupleNormallyCount;
//
//	            } catch (InterruptedException e) {
//	                // TODO Auto-generated catch block
//	                e.printStackTrace();
//	            }
//	        }
//	    }
//		
//		if(!unSolvedMessage.isEmpty()){
//			try {
//				PaymentMessage paymentMessage = unSolvedMessage.take();					
//				solvePayMentmessage(paymentMessage);
//				++DEBUG_sendUnsolvedTupleCount;
//				if (DEBUG_sendUnsolvedTupleCount > 2000000) {
//				    LOG.info("DEBUG_sendUnsolvedTupleCount" + ":" + paymentMessage.toString());
//				}
//
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//		}
//		
//		Long current = System.currentTimeMillis();
//		
//		if(current - TMLastTime > RaceConfig.MinuteBoltInterval){
//		    ++DEBUG_sendEmptyTupleCount;
//			sendEmptyTMPayMessage();
//		}
//		
//		if(current - TBLastTime > RaceConfig.MinuteBoltInterval){
//		    ++DEBUG_sendEmptyTupleCount;
//			sendEmptyTBPayMessage();
//			logDebugInfo();
//		}
//		
//		if (payMessageQueue.isEmpty()
//		        && unSolvedMessage.isEmpty()
//		        && _paymentMsgEndSignal.get()
//		        && current - _latestMsgArrivedTime.get() > CONSUMER_MAX_WAITING_TIME) {
//		    logDebugInfo();
//		    JStormUtils.sleepMs(2000);
//		}
//		
//	}
//
//	@Override
//	public void open(Map conf, TopologyContext arg1, SpoutOutputCollector collector) {
//		// TODO Auto-generated method stub
//		_collector = collector;
//        _sendNumPerNexttuple = JStormUtils.parseInt(
//                conf.get("send.num.each.time"), RaceConfig.DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE);
//		
//		try {
//			initPayConsumer();
//		} catch (MQClientException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}		
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declare) {
//		// TODO Auto-generated method stub
//		declare.declareStream(OldRaceTopology.TMPAYSTREAM, new Fields("orderID", "createTime", "payAmount", "platForm", "source"));
//		declare.declareStream(OldRaceTopology.TBPAYSTREAM, new Fields("orderID", "createTime", "payAmount", "platForm", "source"));
//	}
//
//	@Override
//	public Map<String, Object> getComponentConfiguration() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//	
//	public void logDebugInfo() {
//	    LOG.info("[AllSpout.logDebugInfo] DEBUG_receivedPaymentMsgCount:{}", DEBUG_receivedPaymentMsgCount);
//	    LOG.info("[AllSpout.logDebugInfo] DEBUG_amountEqualsZeroPaymentMsgCount:{}", DEBUG_amountEqualsZeroPaymentMsgCount);
//	    LOG.info("[AllSpout.logDebugInfo] DEBUG_sendTupleNormallyCount:{}", DEBUG_sendTupleNormallyCount);
//	    LOG.info("[AllSpout.logDebugInfo] DEBUG_sendUnsolvedTupleCount:{}", DEBUG_sendUnsolvedTupleCount);
//	    LOG.info("[AllSpout.logDebugInfo] DEBUG_sendEmptyTupleCount:{}", DEBUG_sendEmptyTupleCount);
//	    LOG.info("[AllSpout.logDebugInfo] DEBUG_failedTupleCount:{}", DEBUG_failedTupleCount);
//	}
//
//}
