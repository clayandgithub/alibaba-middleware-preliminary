//package com.alibaba.middleware.unused;
//
//import java.util.Map;
//import java.util.Map.Entry;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.jstorm.RaceTopology;
//import com.alibaba.middleware.race.model.PaymentMessageExt;
//import com.alibaba.middleware.race.rocketmq.CounterFactory;
//import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorHashMap;
//
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.IRichBolt;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//
//public class NewTMMinuteCounter implements IRichBolt, Runnable{
//
//    private static final long serialVersionUID = -6047434323496591058L;
//    private OutputCollector _collector = null;
//    private static final long SEND_TUPLES_INTERVAL = 2000;
//
//    private static Logger LOG = LoggerFactory.getLogger(NewTMMinuteCounter.class);
//	private long lastSendTime = 0;
//	
//	private DecoratorHashMap PCCounter;
//	private DecoratorHashMap WirelessCounter;
//	
//	private int counter = 0;
//
//    @Override
//    public void prepare(Map stormConf, TopologyContext context,
//            OutputCollector collector) {
//        this._collector = collector;
//        this.PCCounter = CounterFactory.createHashCounter();
//        this.WirelessCounter = CounterFactory.createHashCounter();
//        
//        new Thread(this, "NewTMMinuteCounter").start();
//    }
//
//    @Override
//	public void execute(Tuple input) {
//		LOG.info("TMMinute Counter Receive" + ++counter + input.toString());
//		PaymentMessageExt paymentMessageExt = (PaymentMessageExt) input.getValue(0);
//		
//		if(input.getSourceStreamId().equals(RaceTopology.TMPAYSTREAM)){
//            long createTime = paymentMessageExt.getCreateTime();
//            double payAmount = paymentMessageExt.getPayAmount();
//            short payPlatform = paymentMessageExt.getPayPlatform();
//
//            long timeStamp = (createTime / 60000) * 60;
//            if(payPlatform == RaceConfig.PC){
//                PCCounter.put(timeStamp, PCCounter.get(timeStamp) + payAmount);
//            }else{
//                WirelessCounter.put(timeStamp, WirelessCounter.get(timeStamp) + payAmount);
//            }
//        }
//		
//		_collector.ack(input);
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declareStream(RaceTopology.TMPCCOUNTERSTREAM, new Fields("key", "value"));
//		declarer.declareStream(RaceTopology.TMWIRELESSSTREAM, new Fields("key", "value"));
//	}
//
//	@Override
//	public Map<String, Object> getComponentConfiguration() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public void cleanup() {
//		// TODO Auto-generated method stub
//		
//	}
//
//    public void sendTuples() {
//        for(Entry<Long, Double> entry : PCCounter.entrySet()){
//            if(entry.getValue() - 0 > 1e-6){
//                _collector.emit(RaceTopology.TMPCCOUNTERSTREAM, new Values(entry.getKey(), entry.getValue()));//TODO add anchor
//                LOG.info("TMMinuteCounter Emit PCCounter" + entry.getKey() + " : " + entry.getValue());
//            }
//        }
//        CounterFactory.cleanCounter(PCCounter);
//        for(Map.Entry<Long, Double> entry : WirelessCounter.entrySet()){
//            if(entry.getValue() - 0 > 1e-6){
//                _collector.emit(RaceTopology.TMWIRELESSSTREAM, new Values(entry.getKey(), entry.getValue()));//TODO add anchor
//                LOG.info("TMMinuteCounter Emit WirelessCounter" + entry.getKey() + " : " + entry.getValue());
//            }
//        }
//        CounterFactory.cleanCounter(WirelessCounter);
//    }
//
//    @Override
//	public void run(){
//		while(true){
//			sendTuplesIfTimeIsUp();
//			try {
//				Thread.sleep(100);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//	}
//
//    private void sendTuplesIfTimeIsUp() {
//        if(System.currentTimeMillis() - lastSendTime >= SEND_TUPLES_INTERVAL){
//            sendTuples();
//            lastSendTime = System.currentTimeMillis();
//        }
//    }
//}
