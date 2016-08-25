//package com.alibaba.middleware.unused;
//
//import java.util.Map;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.jstorm.RaceTopology;
//import com.alibaba.middleware.race.model.PaymentMessageExt;
//
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.IRichBolt;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//
//public class TBTerminalDistinguish implements IRichBolt {
//	private static final long serialVersionUID = 3835413433259912811L;
//	private OutputCollector _collector = null;
//	private static Logger LOG = LoggerFactory.getLogger(TBTerminalDistinguish.class);
//	private int counter = 0;
//	
//	@Override
//	public void cleanup() {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void execute(Tuple input) {
//		// TODO Auto-generated method stub
//		LOG.info("TBTerminalDistinguish Receive" + ++counter + input.toString());
//		PaymentMessageExt paymentMessageExt = (PaymentMessageExt) input.getValue(0);
//		
//		if(input.getSourceStreamId().equals(RaceTopology.TBPAYSTREAM)){
//            long createTime = paymentMessageExt.getCreateTime();
//            double payAmount = paymentMessageExt.getPayAmount();
//            short payPlatform = paymentMessageExt.getPayPlatform();
//
//            long timeStamp = (createTime / 60000) * 60;
//            if(payPlatform == RaceConfig.PC){
//            	_collector.emit(RaceTopology.TBPCCOUNTERSTREAM, new Values(timeStamp, payAmount));
//                LOG.info("TBTerminalDistinguish Emit PCCounter" + timeStamp + " : " + payAmount);
//            }else{
//            	_collector.emit(RaceTopology.TBWIRELESSSTREAM, new Values(timeStamp, payAmount));
//                LOG.info("TBTerminalDistinguish Emit WirelessCounter" + timeStamp + " : " + payAmount);
//            }
//        }
//		
//		_collector.ack(input);
//	}
//
//	@Override
//	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
//		// TODO Auto-generated method stub
//		_collector = collector;
//
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		// TODO Auto-generated method stub
//		declarer.declareStream(RaceTopology.TBPCCOUNTERSTREAM, new Fields("key", "value"));
//		declarer.declareStream(RaceTopology.TBWIRELESSSTREAM, new Fields("key", "value"));
//
//	}
//
//	@Override
//	public Map<String, Object> getComponentConfiguration() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//}
