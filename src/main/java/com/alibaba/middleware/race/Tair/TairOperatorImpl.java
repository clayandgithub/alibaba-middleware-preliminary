package com.alibaba.middleware.race.Tair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {
	private List<String> configServerList = new ArrayList<String>();
	private DefaultTairManager tairManager = new DefaultTairManager();
	
	private String masterConfigServer;
	private String slaveConfigServer;
	private String groupName;
	private int namespace;
	

    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
    	this.masterConfigServer = masterConfigServer;
    	this.slaveConfigServer = slaveConfigServer;
        this.groupName = groupName;
        this.namespace = namespace;
    	
    	configServerList.add(masterConfigServer);
    	if(slaveConfigServer.length() > 0){
    		configServerList.add(slaveConfigServer);    		
    	}
    	
    	tairManager.setConfigServerList(configServerList);
    	tairManager.setGroupName(groupName);
    	tairManager.init();    	
    }

    public boolean write(Serializable key, Serializable value) {
        ResultCode resultCode = tairManager.put(this.namespace, key, value);
        return resultCode.isSuccess();
    }

    public Object get(Serializable key) {
    	Result<DataEntry> result = tairManager.get(this.namespace, key);
    	if(result.isSuccess()){
    		DataEntry dataEntry = result.getValue();
    		if(dataEntry != null){
    			return dataEntry.getValue();
    		}
    	}
        return null;
    }

    public boolean remove(Serializable key) {
    	ResultCode resultCode = tairManager.delete(this.namespace, key);
    	if(resultCode.isSuccess()){
    		return true;
    	}
        return false;
    }

    public void close(){
    	tairManager.close();
    }

  //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
//        System.out.println(CounterFactory.allTimeStamp);
//        for(Long timeStamp : CounterFactory.allTimeStamp){
        
            Long timeStamp = 1467126060L;
        	System.out.println(timeStamp);
        	String key = RaceConfig.prex_taobao + timeStamp;
        	Double value = (Double) tairOperator.get(key);
        	if(value != null && value - 0 > 1e-6){
        		System.out.println(key + " : " + value);
        	}
        	
        	key = RaceConfig.prex_tmall + timeStamp;
        	value = (Double) tairOperator.get(key);
        	if(value != null && value - 0 > 1e-6){
        		System.out.println(key + " : " + value);
        	}
        	
        	key = RaceConfig.prex_ratio + timeStamp;
        	value = (Double) tairOperator.get(key);
        	if(value != null && value - 0 > 1e-6){
        		System.out.println(key + " : " + value);
        	}
//        }
//        System.out.println("Closing tair");
        tairOperator.close();
    }
}
