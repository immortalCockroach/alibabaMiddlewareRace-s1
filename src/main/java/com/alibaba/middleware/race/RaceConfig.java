package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

	public static final String LogTracker = "ZYTrack8";
	
	public static final String PayIdentifier = "0";
	public static final String TmallIdentifier = "1";
	public static final String TaobaoIdentifier = "2";

	// 这些是写tair key的前缀
	private static final String TeamCode = "41055ps41v";
	public static final String PrexTmall = "platformTmall_" + TeamCode + "_";
	public static final String PrexTaobao = "platformTaobao_" + TeamCode + "_";
	public static final String PrexRatio = "ratio_" + TeamCode + "_";

	// 这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
	public static final String JstormTopologyName = "41055ps41v";
	public static final String RocketMQGroup = "41055ps41v";
	public static final String MqPayTopic = "MiddlewareRaceTestData_Pay";
	public static final String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
	public static final String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

	// public static final int consumerMessageBatchSize = 16;
	// public static final int consumerPullBatchSize = 64;

	// 比赛提交的时候修改为10.101.72.127:5198
	public static final String TairConfigServer = "10.101.72.127:5198";
	// 比赛提交的时候修改为10.101.72.128:5198
	public static final String TairSalveConfigServer = "10.101.72.128:5198";
	// 比赛提交的时候修改为group_tianchi
	public static final String TairGroup = "group_tianchi";

	public static final String Topic = "Topic";
	public static final String Message = "Message";

	// 比赛提交时候修改为64214
	public static final Integer TairNamespace = 64214;

	public static final int processOrderMapInitSegments = 64;

	public static final int processMapEntryArraySize = 32;

	public static final int ThresholdACK = 100000;

	public static final int ThresholdFail = 100000;
}
