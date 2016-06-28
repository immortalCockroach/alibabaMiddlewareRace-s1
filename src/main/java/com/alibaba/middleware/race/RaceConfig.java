package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

	// 这些是写tair key的前缀
	private static String team_code = "41055ps41v";
	public static String prex_tmall = "platformTmall_" + team_code;
	public static String prex_taobao = "platformTaobao_" + team_code;
	public static String prex_ratio = "ratio_" + team_code;

	// 这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
	public static String JstormTopologyName = "41055ps41v";
	public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
	public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
	public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

	// 比赛提交的时候修改为10.101.72.127:5198
	public static String TairConfigServer = "10.109.247.166:5198";
	// 比赛提交的时候修改为10.101.72.128:5198
	public static String TairSalveConfigServer = "xxx";
	// 比赛提交的时候修改为group_tianchi
	public static String TairGroup = "group_1";

	// 比赛提交时候修改为64214
	public static Integer TairNamespace = 1;
}
