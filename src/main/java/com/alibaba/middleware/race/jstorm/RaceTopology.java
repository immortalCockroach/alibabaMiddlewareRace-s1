package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class RaceTopology {

	/**
	 * 最大worker数不超过4,每个worker的task数量不超过6
	 * 计算参考链接http://shiyanjun.cn/archives/1472.html
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub


		Config conf = new Config();

		conf.setNumAckers(1);
		conf.setNumWorkers(4);
		int spout_Parallelism_hint = 2;
		int process_Parallelism_hint = 3;
		int cal_Parallelism_hint = 3;
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 200000);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("topicSpout", new TopicEmitSpout(), spout_Parallelism_hint).setNumTasks(4);

		builder.setBolt("processBolt", new ProcessBolt(), process_Parallelism_hint)
				.fieldsGrouping("topicSpout", new Fields(RaceConfig.Topic)).setNumTasks(6);

		builder.setBolt("CalBolt", new CalAndPersistBolt(), cal_Parallelism_hint)
				.fieldsGrouping("processBolt", new Fields(RaceConfig.Topic)).setNumTasks(6);

		String topologyName = RaceConfig.JstormTopologyName;

		try {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
