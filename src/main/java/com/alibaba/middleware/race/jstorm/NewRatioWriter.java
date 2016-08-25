package com.alibaba.middleware.race.jstorm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.MinuteTimeValueItem;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorHashMap;
import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorTreeMap;
import com.alibaba.middleware.race.util.DoubleUtil;
import com.alibaba.middleware.race.util.FileUtil;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class NewRatioWriter implements IRichBolt {
    private static final long serialVersionUID = -8998720475277834236L;

    private DecoratorHashMap ratioMap;
    private transient LinkedBlockingQueue<MinuteTimeValueItem> pcMinutePayInfoInputQueue;
    private transient LinkedBlockingQueue<MinuteTimeValueItem> wirelessMinutePayInfoInputQueue;

    private static Logger LOG = LoggerFactory.getLogger(NewRatioWriter.class);
    private transient TairOperatorImpl tairOperator;

    private DecoratorTreeMap PCSumCounter;
    private DecoratorTreeMap WirelessSumCounter;

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
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		Long time = tuple.getLong(0);
        Double amount = tuple.getDouble(1);
        if (amount < Constants.DOUBLE_DIFF_THREHOLD) {
            return;
        }
        Short payPlatform = tuple.getShort(2);
        if (payPlatform == Constants.PC) {
            pcMinutePayInfoInputQueue.offer(new MinuteTimeValueItem(time, amount));
        } else {
            wirelessMinutePayInfoInputQueue.offer(new MinuteTimeValueItem(time, amount));
        }
        
//        collector.ack(tuple);//TODO
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
                RaceConfig.TairNamespace);

		pcMinutePayInfoInputQueue = new LinkedBlockingQueue<MinuteTimeValueItem>();
		wirelessMinutePayInfoInputQueue = new LinkedBlockingQueue<MinuteTimeValueItem>();

		PCSumCounter = CounterFactory.createTreeCounter();
        WirelessSumCounter = CounterFactory.createTreeCounter();
        ratioMap = CounterFactory.createHashCounter(Constants.ratioMapInitCapacity);

        new Thread("writeTairThread") {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(Constants.RATIO_WRITE_TAIR_INTERVAL);
                    } catch (InterruptedException e) {
                        LOG.error("writeTairThread", e);
                    }
                    writeTair();
                }
            }
        }.start();

        new Thread("processPCInputQueueThread") {
            @Override
            public void run() {
                while (true) {
                    try {
                        processPCInputQueue();
                    } catch (InterruptedException e) {
                        LOG.error("processPCInputQueueThread", e);
                    }
                }
            }
        }.start();
        
        new Thread("processWirelessInputQueueThread") {
            @Override
            public void run() {
                while (true) {
                    try {
                        processWirelessInputQueue();
                    } catch (InterruptedException e) {
                        LOG.error("processWirelessInputQueue", e);
                    }
                }
            }
        }.start();
	}

    private void writeTair() {
        Double pcSum = 0.0;
        Double wirelessSum = 0.0;
        synchronized (PCSumCounter) {
            synchronized (WirelessSumCounter) {
                for(Map.Entry<Long, Double> entry : PCSumCounter.entrySet()){
                    Long key = entry.getKey();

                    pcSum += PCSumCounter.get(key);
                    wirelessSum += WirelessSumCounter.get(key);

                    if(pcSum > Constants.ZERO_THREHOLD){
                        double ratio = wirelessSum / pcSum;
                        double shortRatio = DoubleUtil.roundedTo2Digit(ratio);
                        if (Math.abs(shortRatio - ratioMap.get(key)) > Constants.RATIO_DIFF_THREHOLD ) {
                            tairOperator.write(RaceConfig.prex_ratio + key, shortRatio);
                            ratioMap.put(key, shortRatio);
//                            FileUtil.appendLineToFile(Constants.DEBUG_FILES_OUTPUT_DIR + "DEBUG_topologyStartTime.txt", RaceConfig.prex_ratio + key + " : " + AllSpout.DEBUG_spoutStartTime);//TODO remove
//                            FileUtil.appendLineToFile(Constants.DEBUG_FILES_OUTPUT_DIR + "ratioTimeMap.txt", RaceConfig.prex_ratio + key + " : " + (System.currentTimeMillis() - AllSpout.DEBUG_spoutStartTime));//TODO remove
//                            FileUtil.appendLineToFile("/home/admin/result.txt", RaceConfig.prex_ratio + key + " : " + shortRatio);//TODO remove
//                            LOG.info("Ratio Writer:" + entryKey + ":" + shortRatio);
                        }
                    }
                }
            }
        }
    }
    
	private void processPCInputQueue() throws InterruptedException {
	    MinuteTimeValueItem item = pcMinutePayInfoInputQueue.take();
	    while (item != null) {
	        synchronized (PCSumCounter) {
	            PCSumCounter.put(item.time, PCSumCounter.get(item.time) + item.value);
	        }
	        item = pcMinutePayInfoInputQueue.take();
	    }
    }
    
    private void processWirelessInputQueue() throws InterruptedException {
        MinuteTimeValueItem item = wirelessMinutePayInfoInputQueue.take();
        while (item != null) {
            synchronized (WirelessSumCounter) {
                WirelessSumCounter.put(item.time, WirelessSumCounter.get(item.time) + item.value);
            }
            item = wirelessMinutePayInfoInputQueue.take();
        }
    }

}
