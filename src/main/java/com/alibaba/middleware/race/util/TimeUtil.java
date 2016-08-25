/**
 * TimeUtil.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.util;

import java.util.Calendar;
import java.util.Random;

/**
 * @author wangweiwei
 *
 */
public class TimeUtil {
    public static Long[] oneDayTimeStamp = new Long[24 * 60];
    public static int curIndex = 0;
    public static Random random = new Random(System.currentTimeMillis());

    static {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2016, Calendar.JUNE, 20, 0, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        
        for(int i = 0; i < oneDayTimeStamp.length; i++){
            oneDayTimeStamp[i] = calendar.getTimeInMillis() / 1000 / 60 * 60;
            calendar.add(Calendar.MINUTE, 1);
        }
    }

    public static long getRandomTimeMillisInOneDay () {
        long minuteTime = oneDayTimeStamp[random.nextInt(24 * 60)];
        return minuteTime * 1000 + random.nextInt(1000);
    }
    
    public static void timeCalculate(String timeMapFile) {
        
    }
}
