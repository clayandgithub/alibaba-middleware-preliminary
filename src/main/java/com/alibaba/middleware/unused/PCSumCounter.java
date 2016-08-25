//package com.alibaba.middleware.unused;
//
//import java.util.Map;
//import java.util.TreeMap;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.jstorm.RaceTopology;
//import com.alibaba.middleware.race.rocketmq.CounterFactory;
//import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorTreeMap;
//
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.BasicOutputCollector;
//import backtype.storm.topology.IBasicBolt;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//
//public class PCSumCounter implements IBasicBolt {	
//	private static final long serialVersionUID = -4494419549882529722L;
//	
//	private static Logger LOG = LoggerFactory.getLogger(PCSumCounter.class);
//	
//	private DecoratorTreeMap sum;
//	private long lastTime = 0;
//	
//	@Override
//	public void execute(Tuple tuple, BasicOutputCollector collector) {
//		// TODO Auto-generated method stub
//		if(tuple.getSourceStreamId().equals(RaceTopology.TBPCCOUNTERSTREAM)
//		   || tuple.getSourceStreamId().equals(RaceTopology.TMPCCOUNTERSTREAM)){
//			Long key = tuple.getLong(0);
//			Double value = tuple.getDouble(1);
//			
//			sum.put(key, sum.get(key) + value);
//		}
//		
//		if(System.currentTimeMillis() - lastTime >= RaceConfig.SumBoltInterval){				
//			for(Map.Entry<Long, Double> entry : sum.entrySet()){
//				if(entry.getValue() - 0 > 1e-6){
//					collector.emit(new Values(entry.getKey(), entry.getValue()));
//					LOG.info("PCSumCounter" + entry.getKey() + " : " + entry.getValue());
//				}
//			}
//			CounterFactory.cleanCounter(sum);				
//			
//			lastTime = System.currentTimeMillis();
//		}
//	}
//
//	@Override
//	public void prepare(Map arg0, TopologyContext arg1) {
//		// TODO Auto-generated method stub
//		sum = CounterFactory.createTreeCounter();
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declare) {
//		// TODO Auto-generated method stub
//		declare.declare(new Fields("key", "value"));
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
//}
