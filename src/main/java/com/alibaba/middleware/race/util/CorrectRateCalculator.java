package com.alibaba.middleware.race.util;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

public class CorrectRateCalculator {
	private static TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
          RaceConfig.TairGroup, RaceConfig.TairNamespace);
	
    private static final double DIFF_THREHOLD = 1e-3;

    private static final Logger LOG = LoggerFactory.getLogger(CorrectRateCalculator.class);

    public static void main(String [] args) throws Exception {
        FileUtil.deleteFileIfExist(Constants.ERROR_RESULT_FILE);
        if (!FileUtil.isFileExist(Constants.EXPECTED_RESULT_FILE)) {
            LOG.error(Constants.EXPECTED_RESULT_FILE + " doesn't exist!");
            return;
        }
//        if (!FileUtil.isFileExist(Constants.ACTUAL_RESULT_FILE)) {
//            LOG.error(Constants.ACTUAL_RESULT_FILE + " doesn't exist!");
//            return;
//        }

        Map<String, Double> expectedResultMap = FileUtil.readHashMapFromFile(Constants.EXPECTED_RESULT_FILE, 5000);
//        Map<String, Double> actualResultMap = FileUtil.readHashMapFromFile(Constants.ACTUAL_RESULT_FILE, 5000);
        
        double correctRate = calculateCorrectRate(expectedResultMap);
        System.out.println("---------------------------------");
        System.out.println("correctRate :" + correctRate);
        System.out.println("---------------------------------");
    }

    /**
     * @param expectedResultMap
     * @param actualResultMap
     */
    private static double calculateCorrectRate(
            Map<String, Double> expectedResultMap) {
//        if (expectedResultMap.size() != actualResultMap.size()) {
//            LOG.warn("expectedResultMap.size() != actualResultMap.size()");
//        }
        int correctCnt = 0;
        for (Entry<String, Double> entry : expectedResultMap.entrySet()) {
        	Double actualValue = (Double) tairOperator.get(entry.getKey());
        	LOG.info("Tair " + entry.getKey() + "=" + actualValue);
        	if(actualValue == null){        		
        		continue;
        	}
            if(Math.abs(entry.getValue() - actualValue) < DIFF_THREHOLD) {
                ++correctCnt;
            } else {
                FileUtil.appendLineToFile(Constants.ERROR_RESULT_FILE, String.format(entry.getKey() + " == expected:%f actual:%f", entry.getValue(), actualValue));
//                LOG.info("expected:{} actual:{}", entry.getValue(), actualValue);
            }
        }
        System.out.println("correctCnt : " + correctCnt);
        return correctCnt * 1.0 / expectedResultMap.size();
    }
}
