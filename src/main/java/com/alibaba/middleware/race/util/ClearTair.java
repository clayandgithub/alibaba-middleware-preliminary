package com.alibaba.middleware.race.util;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

public class ClearTair {
	private static TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
	          RaceConfig.TairGroup, RaceConfig.TairNamespace);

	    private static final Logger LOG = LoggerFactory.getLogger(CorrectRateCalculator.class);

	    public static void main(String [] args) throws Exception {
	        if (!FileUtil.isFileExist(Constants.EXPECTED_RESULT_FILE)) {
	            LOG.error(Constants.EXPECTED_RESULT_FILE + " doesn't exist!");
	            return;
	        }

	        Map<String, Double> expectedResultMap = FileUtil.readHashMapFromFile(Constants.EXPECTED_RESULT_FILE, 5000);
	    
	        for (Entry<String, Double> entry : expectedResultMap.entrySet()) {
                 tairOperator.remove(entry.getKey());
	        	LOG.info("Tair remove" + entry.getKey());
	        }
	    }
}
