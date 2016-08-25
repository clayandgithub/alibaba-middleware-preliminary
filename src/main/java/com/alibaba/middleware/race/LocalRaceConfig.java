package com.alibaba.middleware.race;

import java.io.Serializable;

public class LocalRaceConfig implements Serializable {
    private static final long serialVersionUID = -2421236776869042912L;
    public static String teamcode = "361233bpvz";
    public static String prex_tmall = "platformTmall_" + teamcode + "_";
    public static String prex_taobao = "platformTaobao_" + teamcode + "_";
    public static String prex_ratio = "ratio_" + teamcode + "_";


//    public static String MQNameServerAddr = "192.168.1.101:9876";
/***********Config Value For Local***************/
    public static String JstormTopologyName = "race";
    
    public static String MetaConsumerGroup = "" + System.currentTimeMillis();
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    
    public static String TairConfigServer = "192.168.1.105:5198";
    public static String TairSalveConfigServer = "";
    public static String TairGroup = "group_1";
    public static Integer TairNamespace = 0;        
    
    
    public static short PC = 0;
    public static short Wireless = 1;
    
    public static Long specialTBOrderID = -1L;
    public static Long specialTMOrderID = -2L;
    
    public static long MinuteBoltInterval = 2000L;
    public static long SumBoltInterval = 2000L;
    
    public static int MapInitCapacity = 50000;    
    public static int tradeQueuesize = 10;
    
    public static int MQBatchSize = 1000;
    
    public static final int DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE = 4;

}
