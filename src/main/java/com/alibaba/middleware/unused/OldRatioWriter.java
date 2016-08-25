//package com.alibaba.middleware.unused;
//
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.concurrent.ConcurrentHashMap;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.Constants;
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.Tair.TairOperatorImpl;
//import com.alibaba.middleware.race.model.MetaMessage;
//import com.alibaba.middleware.race.util.DoubleUtil;
//import com.alibaba.middleware.race.util.FileUtil;
//
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.IRichBolt;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Tuple;
//
//public class OldRatioWriter implements IRichBolt {
//    private static final long serialVersionUID = -8998720475277834236L;
//
//    OutputCollector collector;
//    private static final long WRITE_TAIR_INTERVAL = 30000L;
//
//    private static Logger LOG = LoggerFactory.getLogger(OldRatioWriter.class);
//    private transient TairOperatorImpl tairOperator;
//
//    private static ConcurrentHashMap<Long, Double> pcSumCounter ;
//    private static ConcurrentHashMap<Long, Double> wirelessSumCounter ;
//    private Map<Long, Double> tairCache;
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer arg0) {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public Map<String, Object> getComponentConfiguration() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//    @Override
//    public void cleanup() {
//        // TODO Auto-generated method stub
//
//    }
//
//    private synchronized void writeTair(Map<Long, Double> pcSumMap, Map<Long, Double> wirelessSumMap) {
//        List<Long> times = new LinkedList<Long>();
//        Double pcSum = 0.00;
//        Double wirelessSum = 0.00;
//        Iterator<Entry<Long, Double>> iter = pcSumMap.entrySet().iterator();
//        while (iter.hasNext()) {
//            Entry<Long, Double> entry = iter.next();
//            times.add(entry.getKey());
//        }
//        Collections.sort(times);
//        for (Long time : times) {
//            pcSum += pcSumMap.get(time);
//            if (wirelessSumMap.get(time) != null) {
//                wirelessSum += wirelessSumMap.get(time);
//            }
//            if (pcSum > 1e-6) {
//                double ratio = wirelessSum / pcSum;
//                if (tairCache.get(time) != null && tairCache.get(time) - ratio < 1e-6) {
//                    continue;
//                }
//                if (tairOperator.write(RaceConfig.prex_ratio + time, DoubleUtil.roundedTo2Digit(ratio))) {
//                    tairCache.put(time, ratio);
//                }
//                FileUtil.appendLineToFile("/home/admin/result.txt", RaceConfig.prex_ratio + time + " : " + DoubleUtil.roundedTo2Digit(ratio));//TODO remove
//            }
//        }
//    }
//
//	@Override
//	public void execute(Tuple tuple) {
//		// TODO Auto-generated method stub
//		Long time = tuple.getLong(0);
//		MetaMessage paymentMessage = (MetaMessage) tuple.getValue(1);
//        Double thisValue = paymentMessage.getPayAmount();
//
//        if (paymentMessage.getPayPlatform() == RaceConfig.PC) {
//            Double oldValue = pcSumCounter.get(time);
//            if (oldValue == null) {
//                pcSumCounter.put(time, thisValue);
//            } else {
//                pcSumCounter.put(time, oldValue + thisValue);
//            }
//        } else {
//            Double oldValue = wirelessSumCounter.get(time);
//            if (oldValue == null) {
//                wirelessSumCounter.put(time, thisValue);
//            } else {
//                wirelessSumCounter.put(time, oldValue + thisValue);
//            }
//        }
//        
////        collector.ack(tuple);//TODO
//	}
//
//	@Override
//	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
//		// TODO Auto-generated method stub
//		this.collector = collector;
//		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
//                RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
//                RaceConfig.TairNamespace);
//
//        pcSumCounter = new ConcurrentHashMap<Long, Double>();
//        wirelessSumCounter = new ConcurrentHashMap<Long, Double>();
//        
//        tairCache = new HashMap<Long, Double>();
//
//        new Thread() {
//            @Override
//            public void run() {
//                while (true) {
//                    try {
//                        Thread.sleep(WRITE_TAIR_INTERVAL);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    writeTair(pcSumCounter, wirelessSumCounter);
//                }
//            }
//        }.start();
//	}
//
//}
