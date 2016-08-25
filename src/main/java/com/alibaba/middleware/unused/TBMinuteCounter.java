//package com.alibaba.middleware.unused;
//
//import java.util.Map;
//import java.util.Map.Entry;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.jstorm.RaceTopology;
//import com.alibaba.middleware.race.rocketmq.CounterFactory;
//import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorHashMap;
//import com.esotericsoftware.minlog.Log;
//
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.BasicOutputCollector;
//import backtype.storm.topology.IBasicBolt;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//
//public class TBMinuteCounter implements IBasicBolt {
//	private static final long serialVersionUID = -2174576268166494831L;
//
//	private static Logger LOG = LoggerFactory.getLogger(TBMinuteCounter.class);
//	private long lastTime = 0;
//	
//	private DecoratorHashMap PCCounter;
//	private DecoratorHashMap WirelessCounter;
//	
//	private int counter = 0;
//
//	@Override
//	public void execute(Tuple tuple, BasicOutputCollector collector) {
//		// TODO Auto-generated method stub
//		LOG.info("TBMinute Counter Receive" + ++counter + tuple.toString());
//		
//		if(tuple.getSourceStreamId().equals(RaceTopology.TBPAYSTREAM)){
//			long createTime = tuple.getLong(1);
//			double payAmount = tuple.getDouble(2);
//			short payPlatform = tuple.getShort(3);
//			
//
//			long timeStamp = (createTime / 1000 / 60) * 60;
//			if(payPlatform == RaceConfig.PC){
//				PCCounter.put(timeStamp, PCCounter.get(timeStamp) + payAmount);
//			}else{
//				WirelessCounter.put(timeStamp, WirelessCounter.get(timeStamp) + payAmount);
//			}
//		}
//		
//		if(System.currentTimeMillis() - lastTime >= RaceConfig.MinuteBoltInterval){
//			boolean flg = false;
//			
//			for(Entry<Long, Double> entry : PCCounter.entrySet()){
//				if(flg == false){
//					collector.emit(RaceTopology.TBPCCOUNTERSTREAM, new Values(entry.getKey(), entry.getValue()));
//					LOG.info("TBMinuteCounter Emit TBPCCounter" + entry.getKey() + " : " + entry.getValue());
//					
//					flg = true;
//				}else if(entry.getValue() - 0 > 1e-6){
//					collector.emit(RaceTopology.TBPCCOUNTERSTREAM, new Values(entry.getKey(), entry.getValue()));
//					LOG.info("TBMinuteCounter Emit TBPCCounter" + entry.getKey() + " : " + entry.getValue());
//				}
//			}							
//			CounterFactory.cleanCounter(PCCounter);
//			
//			
//			flg = false;
//			for(Map.Entry<Long, Double> entry : WirelessCounter.entrySet()){
//				if(flg == false){
//					collector.emit(RaceTopology.TBWIRELESSSTREAM, new Values(entry.getKey(), entry.getValue()));
//					LOG.info("TBMinuteCounter Emit TBWirelessCounter" + entry.getKey() + " : " + entry.getValue());
//					flg = true;
//				}else if(entry.getValue() - 0 > 1e-6){
//					collector.emit(RaceTopology.TBWIRELESSSTREAM, new Values(entry.getKey(), entry.getValue()));
//					LOG.info("TBMinuteCounter Emit TBWirelessCounter" + entry.getKey() + " : " + entry.getValue());
//				}
//			}
//			CounterFactory.cleanCounter(WirelessCounter);
//			
//			lastTime = System.currentTimeMillis();
//		}
//		
//	}
//
//	@Override
//	public void prepare(Map arg0, TopologyContext arg1) {
//		// TODO Auto-generated method stub
//		PCCounter = CounterFactory.createHashCounter();
//		WirelessCounter = CounterFactory.createHashCounter();
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		// TODO Auto-generated method stub
//		declarer.declareStream(RaceTopology.TBPCCOUNTERSTREAM, new Fields("key", "value"));
//		declarer.declareStream(RaceTopology.TBWIRELESSSTREAM, new Fields("key", "value"));
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
//}
