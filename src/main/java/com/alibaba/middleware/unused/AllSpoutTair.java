//package com.alibaba.middleware.unused;
//
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.LinkedBlockingQueue;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.RaceUtils;
//import com.alibaba.middleware.race.Tair.TairOperatorImpl;
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
//public class AllSpoutTair implements IRichSpout{
//
//	private static final long serialVersionUID = 1685161473482434558L;
//	
//	private static Logger LOG = LoggerFactory.getLogger(AllSpout.class);
//	
//	private SpoutOutputCollector _collector;
//	
//	private int paymentCounter = 0;
//	
//	private long lastTime = 0;
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
//	private transient TairOperatorImpl tairOperator;
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
//					for (MessageExt msg : msgs) {
//
//					     byte [] body = msg.getBody();
//					     if (body.length == 2 && body[0] == 0 && body[1] == 0) {
//					         System.out.println("Got the end signal");
//					         continue;
//					     }
//					     
//					     if(msg.getTopic().equals(RaceConfig.MqPayTopic)){
//					    	 PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
//						     payMessageQueue.put(paymentMessage); 
//					     }else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
//					    	 OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//						     TBTradeMessage.put(orderMessage.getOrderId(), orderMessage.getTotalPrice());
//						     if(TBTradeMessage.size() > RaceConfig.tradeQueuesize){
//						    	 reduceTradeMessage(TBTradeMessage, true);
//						     }
//					     }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
//					    	 OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//						     TMTradeMessage.put(orderMessage.getOrderId(), orderMessage.getTotalPrice());
//						     if(TMTradeMessage.size() > RaceConfig.tradeQueuesize){
//						    	 reduceTradeMessage(TMTradeMessage, false);
//						     }
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
//	private void sendEmptyPayMessage(){
//		long TMOrderID = RaceConfig.specialTMOrderID;		
//		PaymentMessage paymentMessage = new PaymentMessage(TMOrderID, 0.0, (short)0, RaceConfig.PC, CounterFactory.timeStamp * 1000);
//		solvePayMentmessage(paymentMessage);
//		
//		paymentMessage = new PaymentMessage(TMOrderID, 0.0, (short)0, RaceConfig.Wireless, CounterFactory.timeStamp * 1000);
//		solvePayMentmessage(paymentMessage);
//		
//		long TBOrderID = RaceConfig.specialTBOrderID;
//		paymentMessage = new PaymentMessage(TBOrderID, 0.0, (short)0, RaceConfig.PC, CounterFactory.timeStamp * 1000);
//		solvePayMentmessage(paymentMessage);
//		
//		paymentMessage = new PaymentMessage(TBOrderID, 0.0, (short)0, RaceConfig.Wireless, CounterFactory.timeStamp * 1000);
//		solvePayMentmessage(paymentMessage);
//	}
//
//	private boolean solvePayMentmessage(PaymentMessage paymentMessage){
//		paymentCounter++;
//		
//		Long orderID = paymentMessage.getOrderId();
//		
//		if(TMTradeMessage.containsKey(orderID)){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TMPAYSTREAM, values, paymentMessage);
//			
//			lastTime = System.currentTimeMillis();
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
//			return true;
//		}else if(TBTradeMessage.containsKey(orderID)){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TBPAYSTREAM, values, paymentMessage);
//			
//			lastTime = System.currentTimeMillis();			
//			Double lastAmount = TBTradeMessage.get(orderID);
//			if(lastAmount - paymentMessage.getPayAmount() < 1e-6){
//				TBTradeMessage.remove(orderID);
//				completeTBTrade.put(orderID, 0.0);
//			}else{
//				TBTradeMessage.put(orderID, lastAmount - paymentMessage.getPayAmount());
//			}
//			LOG.info("AllSpout Emit TBPayment" + paymentCounter + ":" + paymentMessage.toString());
//			return true;
//		}else{
//			return false;
//		}
//	}
//	
//	private boolean solvePaymentMessageByTair(PaymentMessage paymentMessage){
//
//		paymentCounter++;
//		
//		Long orderID = paymentMessage.getOrderId();
//		String v = (String)tairOperator.get(orderID);
//		
//		if(v != null && v.equals("TM")){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TMPAYSTREAM, values, paymentMessage);
//			
//			lastTime = System.currentTimeMillis();
//			LOG.info("AllSpout Emit TMPayment" + paymentCounter + ":" + paymentMessage.toString());
//			return true;
//		}else if(v != null && v.equals("TB")){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TBPAYSTREAM, values, paymentMessage);
//			LOG.info("AllSpout Emit TBPayment" + paymentCounter + ":" + paymentMessage.toString());
//			return true;
//		}else{
//			unSolvedMessage.add(paymentMessage);
//			return false;
//		}
//	
//	}
//	
//	private void solveFailPaymentMessage(PaymentMessage paymentMessage){
//		Long orderID = paymentMessage.getOrderId();
//		
//		if(TMTradeMessage.containsKey(orderID) || 
//				completeTMTrade.containsKey(orderID) ||
//					((String)tairOperator.get(orderID)).equals("TM")){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TMPAYSTREAM, values, paymentMessage);
//			
//			lastTime = System.currentTimeMillis();			
//			LOG.info("AllSpout Emit TMPayment" + paymentCounter + ":" + paymentMessage.toString());
//		}else if(TBTradeMessage.containsKey(orderID) ||	
//					completeTBTrade.containsKey(orderID) ||
//						((String)tairOperator.get(orderID)).equals("TB") ){
//			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
//					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
//			_collector.emit(OldRaceTopology.TBPAYSTREAM, values, paymentMessage);
//			
//			lastTime = System.currentTimeMillis();			
//			LOG.info("AllSpout Emit TBPayment" + paymentCounter + ":" + paymentMessage.toString());
//		}else{
//			unSolvedMessage.add(paymentMessage);
//		}
//	}
//	
//	private void reduceTradeMessage(ConcurrentHashMap<Long, Double> map, boolean isTaobao){
//		Iterator<Long> ite = map.keySet().iterator();
//		if(isTaobao){
//			for(int i = 0; i < map.size() / 2; i++){
//				Long key = ite.next();
//				tairOperator.write(key, "TB");
//				ite.remove();
//			}
//		}else{
//			for(int i = 0; i < map.size() / 2; i++){
//				Long key = ite.next();
//				tairOperator.write(key, "TM");
//				ite.remove();
//			}
//		}
//		
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
//		solveFailPaymentMessage((PaymentMessage)paymentMessage);
//	}
//
//	@Override
//	public void nextTuple() {
//		// TODO Auto-generated method stub
//		if(!payMessageQueue.isEmpty()){
//			try {
//				PaymentMessage paymentMessage = payMessageQueue.take();				
//				if(!solvePayMentmessage(paymentMessage)){
//					solvePaymentMessageByTair(paymentMessage);
//				}
//				
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		
//		if(!unSolvedMessage.isEmpty()){
//			try {
//				PaymentMessage paymentMessage = unSolvedMessage.take();					
//				solvePayMentmessage(paymentMessage);
//				
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//		}
//		
//		if(System.currentTimeMillis() - lastTime > RaceConfig.MinuteBoltInterval){
//			sendEmptyPayMessage();
//		}
//	}
//
//	@Override
//	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//		// TODO Auto-generated method stub
//		_collector = collector;
//		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
//                RaceConfig.TairGroup, RaceConfig.TairNamespace);
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
//}
