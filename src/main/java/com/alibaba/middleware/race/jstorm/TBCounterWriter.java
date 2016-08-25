package com.alibaba.middleware.race.jstorm;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorHashMap;
import com.alibaba.middleware.race.util.DoubleUtil;
import com.alibaba.middleware.race.util.FileUtil;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import io.netty.util.internal.ConcurrentSet;

public class TBCounterWriter implements IBasicBolt, Runnable{
	private static final long serialVersionUID = -6569019778720857581L;

	private static Logger LOG = LoggerFactory.getLogger(TBCounterWriter.class);

	private transient TairOperatorImpl tairOperator;
	private DecoratorHashMap sum;
	
	private ConcurrentSet<Long> receivedKeySet;
	
	private void writeTBCounter(){
	    synchronized (receivedKeySet) {
	        for(Long key : receivedKeySet){
    	        tairOperator.write(RaceConfig.prex_taobao + key, DoubleUtil.roundedTo2Digit(sum.get(key)));
//                LOG.info("TBCounterWriter: " + RaceConfig.prex_taobao + key + " " + sum.get(key));
//    	        FileUtil.appendLineToFile(Constants.DEBUG_FILES_OUTPUT_DIR + "DEBUG_topologyStartTime.txt", RaceConfig.prex_taobao + key + " : " + AllSpout.DEBUG_spoutStartTime);//TODO remove
//    	        FileUtil.appendLineToFile(Constants.DEBUG_FILES_OUTPUT_DIR + "tbTimeMap.txt", RaceConfig.prex_taobao + key + " : " + (System.currentTimeMillis() - AllSpout.DEBUG_spoutStartTime));//TODO remove
//                FileUtil.appendLineToFile("/home/admin/result.txt", RaceConfig.prex_taobao + key + " : " + sum.get(key));//TODO remove
	        }
	        receivedKeySet.clear();
	    }
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Long time = tuple.getLong(0);
		Double amount = tuple.getDouble(1);
		synchronized (receivedKeySet) {
		    receivedKeySet.add(time);
		    sum.put(time, sum.get(time) + amount);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		
		sum = CounterFactory.createHashCounter(Constants.sumCounterMapInitCapacity);
		receivedKeySet = new ConcurrentSet<Long>();
		new Thread(this, "TBCounterWriter").start();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			try {
				Thread.sleep(Constants.TM_OR_TB_COUNTER_WRITE_TAIR_INTERVAL);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			writeTBCounter();
		}
	}

}
