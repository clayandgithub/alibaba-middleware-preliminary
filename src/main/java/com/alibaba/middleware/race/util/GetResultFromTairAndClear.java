package com.alibaba.middleware.race.util;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.util.FileUtil;

public class GetResultFromTairAndClear {

    private static final Logger LOG = LoggerFactory.getLogger(GetResultFromTairAndClear.class);

    public static void main(String [] args) throws Exception {
        FileUtil.deleteFileIfExist(Constants.ACTUAL_RESULT_FILE);
        if (!FileUtil.isFileExist(Constants.EXPECTED_RESULT_FILE)) {
            LOG.error(Constants.EXPECTED_RESULT_FILE + " doesn't exist!");
            return;
        }
        Map<String, Double> expectedResultMap = FileUtil.readHashMapFromFile(Constants.EXPECTED_RESULT_FILE, 5000);
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        
        System.out.println("Starting...");
        for(Entry<String, Double> entry : expectedResultMap.entrySet()){
        	String key = entry.getKey();
        	Double value = (Double) tairOperator.get(key);
        	outputResult(key, value);
            tairOperator.remove(key);
        }
        tairOperator.close();
        System.out.println("End!");
        System.exit(0);
    }

    private static void outputResult(String key, Double value) {
//        System.out.println(key + " : " + value);
        if (value != null && value.compareTo(0.0) > 0) {
            FileUtil.appendLineToFile(Constants.ACTUAL_RESULT_FILE, key + " : " + value);
        }
    }
}
