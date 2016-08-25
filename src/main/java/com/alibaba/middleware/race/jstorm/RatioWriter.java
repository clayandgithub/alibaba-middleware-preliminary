package com.alibaba.middleware.race.jstorm;

import io.netty.util.internal.ConcurrentSet;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorTreeMap;
import com.alibaba.middleware.race.util.DoubleUtil;
import com.alibaba.middleware.race.util.FileUtil;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class RatioWriter implements IRichBolt {
    private static final long serialVersionUID = -8998720475277834236L;

    OutputCollector collector;


    private static Logger LOG = LoggerFactory.getLogger(RatioWriter.class);
    private transient TairOperatorImpl tairOperator;

    private DecoratorTreeMap PCSumCounter;
    private DecoratorTreeMap WirelessSumCounter;
    private Set<Long> receivedKeySet;

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

    private void writeTair() {
        synchronized (receivedKeySet) {
            Double pcSum = 0.0;
            Double wirelessSum = 0.0;
            
            for(Long key : receivedKeySet){
                pcSum += PCSumCounter.get(key);
                wirelessSum += WirelessSumCounter.get(key);
                
                if(pcSum > Constants.ZERO_THREHOLD){
                    double ratio = wirelessSum / pcSum;
                    tairOperator.write(RaceConfig.prex_ratio + key, DoubleUtil.roundedTo2Digit(ratio));
//                  FileUtil.appendLineToFile("/home/admin/result.txt", RaceConfig.prex_ratio + key + " : " + DoubleUtil.roundedTo2Digit(ratio));//TODO remove
//                    LOG.info("Ratio Writer:" + entryKey + ":" + WirelessSumCounter.get(entryKey) / PCSumCounter.get(entryKey));
                }
            }
            receivedKeySet.clear();
        }
    }

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		Long time = tuple.getLong(0);
        Double amount = tuple.getDouble(1);
        if (amount < Constants.DOUBLE_DIFF_THREHOLD) {
            return;
        }
        Short payPlatform = tuple.getShort(2);
        
        synchronized (receivedKeySet) {
            receivedKeySet.add(time);
            if (payPlatform == Constants.PC) {
                PCSumCounter.put(time, PCSumCounter.get(time) + amount);
            } else {
                WirelessSumCounter.put(time, WirelessSumCounter.get(time) + amount);
            }
        }
//        collector.ack(tuple);//TODO
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
                RaceConfig.TairNamespace);

		PCSumCounter = CounterFactory.createTreeCounter();
        WirelessSumCounter = CounterFactory.createTreeCounter();
        
        receivedKeySet = new ConcurrentSet<Long>();
        receivedKeySet.clear();

        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(Constants.RATIO_WRITE_TAIR_INTERVAL);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    writeTair();
                }
            }
        }.start();
	}

}
