//package com.alibaba.middleware.unused;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.alibaba.middleware.race.RaceConfig;
//import com.alibaba.middleware.race.jstorm.AllSpout;
//import com.alibaba.middleware.race.jstorm.PlatformDistinguish;
//import com.alibaba.middleware.race.jstorm.TBCounterWriter;
//import com.alibaba.middleware.race.jstorm.TMCounterWriter;
//
//import backtype.storm.Config;
//import backtype.storm.StormSubmitter;
//import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.tuple.Fields;
//
///**
// * 这是一个很简单的例子
// * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
// * 我们会将选手拓扑杀掉。
// */
//
///**
// * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
// * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
// * 所以这个主类路径一定要正确
// */
//public class OldRaceTopology {
//    private static Logger LOG = LoggerFactory.getLogger(OldRaceTopology.class);
//    /** Spout **/
//    private static final int AllSpoutParallelism = 4;
//    public static final String ALLSPOUT = "AllSpout";    
//    
//    /** Platform Distinguish **/
//    private static final int PlatformParallelism = 8;
//    public static final String PLATFORMBOLT = "PlatformBolt";  
//    public static final String TMPAYSTREAM = "TMPayStream";
//    public static final String TBPAYSTREAM = "TBPayStream";
//    
//    /** PaymentMessageFilter **/
//    private static final int PaymentMessageFilterParallelism = 4;
//    public static final String PAY_MSG_FILTER_BOLT = "PaymentMessageFilter";
//    
//    /** Writer Bolt **/
//    private static final int TMCounterWriterParallelism = 4;
//    public static final String TMCOUNTERWRITERBOLT = "TMCounterWriter";
//    
//    private static final int TBCounterWriterParallelism = 4;
//    public static final String TBCOUNTERWRITERBOLT = "TBCounterWriter";
//    
//    private static final int RationCounterParallelism = 6;
//    public static final String RATIONWRITERBOLT = "RatioWriter";
//    
//    
//    
//    public static void main(String[] args){
//
//        TopologyBuilder builder = new TopologyBuilder();
//
//        /** Spout **/        
//        builder.setSpout(ALLSPOUT, new AllSpout(), AllSpoutParallelism);
//        
//        /**Bolts receive tuples form spout**/
//        builder.setBolt(PLATFORMBOLT, new PlatformDistinguish(), PlatformParallelism)
//               .fieldsGrouping(ALLSPOUT, new Fields("orderID"));
//        builder.setBolt(PAY_MSG_FILTER_BOLT, new PaymentMessageFilter(), PaymentMessageFilterParallelism)
//            .shuffleGrouping(ALLSPOUT);
//
//        /** tm/tb Writer Bolt **/
//        builder.setBolt(TMCOUNTERWRITERBOLT, new TMCounterWriter(), TMCounterWriterParallelism)
// 	   		   .fieldsGrouping(PLATFORMBOLT, TMPAYSTREAM, new Fields("time"));
//        builder.setBolt(TBCOUNTERWRITERBOLT, new TBCounterWriter(), TBCounterWriterParallelism)
// 	           .fieldsGrouping(PLATFORMBOLT, TBPAYSTREAM, new Fields("time"));
//
//        /** ratio writer**/
//        builder.setBolt(RATIONWRITERBOLT, new OldRatioWriter(), RationCounterParallelism)
//               .fieldsGrouping(PAY_MSG_FILTER_BOLT, new Fields("time"));
//
//        String topologyName = RaceConfig.JstormTopologyName;
//
//        Config conf = new Config();
//        conf.setNumWorkers(4);
//        conf.setNumAckers(0);
//        
////      conf.setMessageTimeoutSecs(90);
////        conf.setMaxSpoutPending(RaceConfig.SpoutMaxPending);
//        
//        try {
//            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
//}