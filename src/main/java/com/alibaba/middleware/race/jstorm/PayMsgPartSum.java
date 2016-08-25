package com.alibaba.middleware.race.jstorm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PayMsgPartSum implements IRichBolt {
    private static final long serialVersionUID = -3103867690922937400L;

    private static Logger LOG = LoggerFactory.getLogger(PayMsgPartSum.class);

    private DecoratorHashMap pcSumCounter;
    private static DecoratorHashMap wirelessSumCounter ;

	private OutputCollector _collector;
	@Override
    public void execute(Tuple tuple) {
	       Long time = tuple.getLong(0);
	       Double amount = tuple.getDouble(1);
	       Short payPlatform = tuple.getShort(2);
	       if (Constants.PC == payPlatform) {
	           synchronized (pcSumCounter) {
	               pcSumCounter.put(time, pcSumCounter.get(time) + amount);
	           }
	       } else {
	           synchronized (wirelessSumCounter) {
	               wirelessSumCounter.put(time, wirelessSumCounter.get(time) + amount);
	           }
	       }
//        _collector.ack(tuple);//TODO
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "amount", "payPlatform"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        pcSumCounter = CounterFactory.createHashCounter(Constants.sumCounterMapInitCapacity);
        wirelessSumCounter = CounterFactory.createHashCounter(Constants.sumCounterMapInitCapacity);
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(Constants.PAY_MSG_PART_SUM_SEND_INTERVAL);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    sendSums();
                }
            }
        }.start();
    }
    
    private void sendSums() {
        synchronized (pcSumCounter) {
            for (Map.Entry<Long, Double> entry : pcSumCounter.entrySet()) {
                if (entry.getValue() > Constants.ZERO_THREHOLD) {
                    _collector.emit(new Values(entry.getKey(), entry.getValue(), Constants.PC));
                }
            }
            CounterFactory.cleanCounter(pcSumCounter);
        }

        synchronized (wirelessSumCounter) {
            for (Map.Entry<Long, Double> entry : wirelessSumCounter.entrySet()) {
                if (entry.getValue() > Constants.ZERO_THREHOLD) {
                    _collector.emit(new Values(entry.getKey(), entry.getValue(), Constants.WIRELESS));
                }
            }
            CounterFactory.cleanCounter(wirelessSumCounter);
        }
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
