//package com.alibaba.middleware.unused;
//
//import java.util.Map;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.jstorm.utils.JStormUtils;
//import com.alibaba.middleware.race.jstorm.RaceTopology;
//import com.alibaba.middleware.race.rocketmq.CounterFactory;
//import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorTreeMap;
//
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.IRichBolt;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//
//public class NewPCSumCounter implements IRichBolt, Runnable{
//
//    private static final long serialVersionUID = 6339967899780764770L;
//    private OutputCollector _collector = null;
//    private static final long SEND_TUPLES_INTERVAL = 2000;
//
//    private static Logger LOG = LoggerFactory.getLogger(NewPCSumCounter.class);
//	private long lastSendTime = 0;
//	
//	private DecoratorTreeMap sum = null;
//
//	private transient LinkedBlockingQueue<Tuple> _inputTuples;
//	
//	private int counter = 0;
//
//    @Override
//    public void prepare(Map stormConf, TopologyContext context,
//            OutputCollector collector) {
//        this._collector = collector;
//        this._inputTuples = new LinkedBlockingQueue<Tuple>();
//        sum = CounterFactory.createTreeCounter();
//        new Thread(this, "NewPCSumCounterProcessTuples").start();
//    }
//
//    @Override
//	public void execute(Tuple input) {
//		LOG.info("NewPCSumCounter Counter Receive" + ++counter + input.toString());
//		try {
//            _inputTuples.put(input);
//            _collector.ack(input);//TODO
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//	    declarer.declare(new Fields("key", "value"));
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
//	public void sendTuples() {
//	    for(Map.Entry<Long, Double> entry : sum.entrySet()){
//            if(entry.getValue() - 0 > 1e-6){
//                _collector.emit(new Values(entry.getKey(), entry.getValue()));
//                LOG.info("NewPCSumCounter" + entry.getKey() + " : " + entry.getValue());
//            }
//        }
//        CounterFactory.cleanCounter(sum);
//    }
//
//    @Override
//    public void run() {
//        while (true) {
//            Tuple tuple = _inputTuples.poll();
//            while (tuple != null) {
//                if(tuple.getSourceStreamId().equals(RaceTopology.TBPCCOUNTERSTREAM)
//                        || tuple.getSourceStreamId().equals(RaceTopology.TMPCCOUNTERSTREAM)){
//                     Long key = tuple.getLong(0);
//                     Double value = tuple.getDouble(1);
//                     
//                     sum.put(key, sum.get(key) + value);
//                 }
//                sendTuplesIfTimeIsUp();
//                tuple = _inputTuples.poll();
//            }
//            sendTuplesIfTimeIsUp();
//            JStormUtils.sleepMs(10);//TODO remove
//        }
//    }
//
//    private void sendTuplesIfTimeIsUp() {
//        if(System.currentTimeMillis() - lastSendTime >= SEND_TUPLES_INTERVAL){
//            sendTuples();
//            lastSendTime = System.currentTimeMillis();
//        }
//    }
//}
