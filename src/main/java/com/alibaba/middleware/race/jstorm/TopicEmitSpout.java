package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.ConsumerFactory;
import com.alibaba.middleware.race.model.MetaTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TopicEmitSpout implements IRichSpout, MessageListenerConcurrently, IAckValueSpout, IFailValueSpout {

	private static final Logger logger = LoggerFactory.getLogger(TopicEmitSpout.class);
	private SpoutOutputCollector spoutCollector;
	private DefaultMQPushConsumer consumer;
	private LinkedBlockingDeque<MetaTuple> emitQueue;
	private boolean flowControl;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		spoutCollector = collector;
		try {
			consumer = ConsumerFactory.mkPushConsumerInstance(this);
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			logger.error("failed to create pushconsumer", e);
		}
		emitQueue = new LinkedBlockingDeque<>();
		flowControl = false;
	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

		String topic;
		byte[] body;
		for (MessageExt msg : msgs) {
			topic = msg.getTopic();
			body = msg.getBody();

			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				logger.info(topic + "ends soon");
				continue;
			}

			switch (topic) {
			case RaceConfig.MqPayTopic:
				PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
				if (flowControl) {
					emitQueue.offer(new MetaTuple("0", paymentMessage));
				} else {
					sendMetaTuple(new MetaTuple("0", paymentMessage));
				}
				break;
			case RaceConfig.MqTmallTradeTopic:
				OrderMessage tmallMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
				if (flowControl) {
					emitQueue.offer(new MetaTuple("1", tmallMessage));
				} else {
					sendMetaTuple(new MetaTuple("1", tmallMessage));
				}
				break;
			case RaceConfig.MqTaobaoTradeTopic:
				OrderMessage taobaoMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
				if (flowControl) {
					emitQueue.offer(new MetaTuple("2", taobaoMessage));
				} else {
					sendMetaTuple(new MetaTuple("2", taobaoMessage));
				}
				break;
			default:
				logger.error("contains topic except of 3 above" + topic);
				break;
			}
		}
		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	}

	public void sendMetaTuple(MetaTuple tuple) {
		List<Object> values = new Values(tuple.getTopic(), tuple.getMessage());

		spoutCollector.emit(values, tuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(RaceConfig.Topic, RaceConfig.Message));
	}

	@Override
	public void nextTuple() {
		MetaTuple metaTuple = null;
		try {
			metaTuple = emitQueue.take();
		} catch (InterruptedException e) {
		}

		if (metaTuple == null) {
			return;
		}

		sendMetaTuple(metaTuple);

	}

	@Override
	public void fail(Object msgId, List<Object> values) {
		// TODO Auto-generated method stub

	}

	@Override
	public void ack(Object msgId, List<Object> values) {
		// TODO Auto-generated method stub

	}

	@Deprecated
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
	}

	@Deprecated
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
