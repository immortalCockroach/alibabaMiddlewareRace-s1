package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class RaceTopology {

	/**
	 * 最大worker数不超过4,每个worker的task数量不超过6
	 * 计算参考链接http://shiyanjun.cn/archives/1472.html
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Config conf = new Config();
		
		conf.setNumAckers(1);
		conf.setNumWorkers(3);
        int spout_Parallelism_hint = 4;
        int split_Parallelism_hint = 4;
        int count_Parallelism_hint = 4;
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 200000);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("topicSpout", new TopicEmitSpout(),spout_Parallelism_hint);
        
        String topologyName = RaceConfig.JstormTopologyName;

        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}

}
