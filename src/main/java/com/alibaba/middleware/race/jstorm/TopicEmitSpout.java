package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

public class TopicEmitSpout implements IRichSpout,MessageListenerConcurrently{

	private static final Logger logger = LoggerFactory.getLogger(TopicEmitSpout.class);
	private SpoutOutputCollector spoutCollector;
	private DefaultMQPushConsumer consumer;
	
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub	
	}


	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		spoutCollector = collector;
	}



	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
		// TODO Auto-generated method stub
		return null;
	}
	

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub	
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub	
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub	
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub	
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
}
