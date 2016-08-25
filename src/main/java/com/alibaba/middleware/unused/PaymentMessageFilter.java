//package com.alibaba.middleware.unused;
//
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.LinkedBlockingQueue;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.model.MetaMessage;
//
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.IRichBolt;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//
//public class PaymentMessageFilter implements IRichBolt {
//	private static Logger LOG = LoggerFactory.getLogger(PaymentMessageFilter.class);
//	
//	private static final long serialVersionUID = -8918483233950498761L;
//	private OutputCollector _collector;
//	@Override
//    public void execute(Tuple tuple) {
//        //System.out.println("==============PayPlatformBolt bolt begin==========");
//        MetaMessage metaMessage = (MetaMessage) tuple.getValue(1);
//        if (metaMessage != null
//                && RaceConfig.MqPayTopic.equals(metaMessage.getTopic())) {
//            long time = metaMessage.getCreateTime() / 60000 * 60;
//            _collector.emit(new Values(time, metaMessage));
//        }
////        _collector.ack(tuple);//TODO
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("time", "metaMessage"));
//    }
//
//    @Override
//    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
//        this._collector = collector;
//    }
//
//    @Override
//    public void cleanup() {
//        // TODO Auto-generated method stub
//
//    }
//
//    @Override
//    public Map<String, Object> getComponentConfiguration() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//}
