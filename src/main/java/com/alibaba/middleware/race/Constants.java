/**
 * Constants.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race;

/**
 * @author wangweiwei
 *
 */
public class Constants {
//    public static final String HOME_PATH = "Z:/AliProject/AliMiddlewareSolution/tmpfile/";
    public static final String HOME_PATH = "/home/clayandwind/";
    public static final String DEBUG_FILES_OUTPUT_DIR = "/home/admin/";
    public static final String EXPECTED_RESULT_FILE = HOME_PATH + "expectedResult.txt";
    public static final String ACTUAL_RESULT_FILE = HOME_PATH + "actualResult.txt";
    public static final String ERROR_RESULT_FILE = HOME_PATH + "errorResult.txt";
    
    public static final double DOUBLE_DIFF_THREHOLD = 1e-6;
    public static final double RATIO_DIFF_THREHOLD = 1e-3;
    public static final double ZERO_THREHOLD = 1e-6;

    public static final short TAOBAO = 0;
    public static final short TMALL = 1;

    public static final short PC = 0;
    public static final short WIRELESS = 1;
    
    // configure constants for spout
    public static int MQBatchSize = 1024;
    public static int SPOUT_MAX_SEND_NUMBER_PER_NEXT_TUPLE = 50000;
    // 此时间内收不到任何消息，且_paymentMsgEndSignal为true,则认为所有消息接收完成
    public static final long SPOUT_CONSUMER_MAX_WAITING_TIME = 1 * 60 * 1000L;
    public static final long EMPTY_SEND_QUEUE_SLEEP_TIME = 1L;
    public static final long CONSUMER_TIMEOUT_SLEEP_TIME = 1000L;
    public static final long NO_MORE_MESSAGE_SLEEP_TIME = 10000L;
    
    // configure constants for bolts
    public static long PAY_MSG_PART_SUM_SEND_INTERVAL = 5000L;
    public static long PLATFORM_DISTINGUISH_EMPTY_QUEUE_SLEEP_TIME = 1000L;
    public static long RATIO_WRITE_TAIR_INTERVAL = 5000L;
    public static long TM_OR_TB_COUNTER_WRITE_TAIR_INTERVAL = 5000L;

    public static int sumCounterMapInitCapacity = 50000;
    public static int ratioMapInitCapacity = 50000;
    public static int receivedPayMsgIdSetInitCapacity = 100000;
    public static int tradeMsgMapInitCapacity = 100000;
    

}
