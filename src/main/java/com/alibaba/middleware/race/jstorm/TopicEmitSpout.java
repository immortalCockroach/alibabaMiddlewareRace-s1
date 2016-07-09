package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
	private transient DefaultMQPushConsumer consumer;
	private LinkedBlockingQueue<MetaTuple> emitQueue;
	// flowControl只针对pay消息,防止pay消息过多而在ProcessBolt中产生堆积（而Order消息必须堆积，所以暂时不考虑流量控制）
	private volatile boolean flowControl;

	// 在flowControl模式下，ackNumber累计到一定程度就取消flowControl
	private AtomicInteger ackNumber;

	// 在非flowControl模式下 failNumber累积到一定程度就重新设置flowControl
	private AtomicInteger failNumber;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		spoutCollector = collector;
		try {
			consumer = ConsumerFactory.mkPushConsumerInstance(this);
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			logger.error(RaceConfig.LogTracker + "ZY spout failed to create pushconsumer", e);
		}
		emitQueue = new LinkedBlockingQueue<MetaTuple>();
		// 初始设置flowControl为true 防止一开始pay消息发送过多，在ProcessBolt中找不到对应的Order而产生fail信息
		flowControl = true;

		ackNumber = new AtomicInteger(0);
		failNumber = new AtomicInteger(0);

		logger.info(RaceConfig.LogTracker + "ZY spout init finished.");

	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

		String topic;
		byte[] body;
		for (MessageExt msg : msgs) {
			topic = msg.getTopic();
			body = msg.getBody();

			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				logger.info(RaceConfig.LogTracker + "ZY spout" + topic + "ends soon");
				continue;
			}

			switch (topic) {
			case RaceConfig.MqPayTopic:
				// 用字符串0代表pay消息
				PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
				// 在flowControl模式下 使用阻塞队列来发送Pay的信息
				if (flowControl) {
					try {
						emitQueue.put(new MetaTuple(paymentMessage));
					} catch (InterruptedException e) {
						logger.info(RaceConfig.LogTracker + "ZY spout put PAY operation interupt:" + e.getMessage(), e);
					}
				} else {
					sendPayMessage(new MetaTuple(paymentMessage));
				}
				break;

			case RaceConfig.MqTmallTradeTopic:
				// 1代表Tmall消息
				OrderMessage tmallMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
				sendOrderMessage(RaceConfig.TmallIdentifier, tmallMessage);
				break;

			case RaceConfig.MqTaobaoTradeTopic:
				// 2带代表Taobao消息
				OrderMessage taobaoMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
				sendOrderMessage(RaceConfig.TaobaoIdentifier, taobaoMessage);
				break;

			default:
				logger.error(RaceConfig.LogTracker + "ZY spout contains topic except of 3 above" + topic);
				break;
			}
		}
		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	}

	/**
	 * Pay的发送方式，可能是直接发送或者阻塞队列发送(由于可能重发，因此需要设置failTime等)，发送的数据格式为Id MetaTuple
	 * 
	 * @param tuple
	 */
	private void sendPayMessage(MetaTuple payMessage) {
		List<Object> values = new Values(RaceConfig.PayIdentifier, payMessage);

		// 将MetaTuple作为messageId
		spoutCollector.emit(values, payMessage);
	}

	/**
	 * 订单的发送方式， 发送的数据格式为Id OrderMessage
	 * 
	 * @param topicIdentifier
	 * @param message
	 */
	private void sendOrderMessage(String topicIdentifier, OrderMessage message) {
		List<Object> values = new Values(topicIdentifier, message);
		// 将Message对象作为messageId
		spoutCollector.emit(values, message);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(RaceConfig.Topic, RaceConfig.Message));
	}

	@Override
	public void nextTuple() {
		MetaTuple payMessage = null;
		try {
			payMessage = emitQueue.take();
		} catch (InterruptedException e) {
			logger.info(RaceConfig.LogTracker + "ZY spout take operation interupt:" + e.getMessage(), e);
		}

		if (payMessage == null) {
			return;
		}

		sendPayMessage(payMessage);

	}

	@Override
	public void fail(Object msgId, List<Object> values) {
		// 如果非flowControl下 failNum数量过大，且超过20%则进入flowControl模式
		if (!flowControl) {
			int failNum = failNumber.incrementAndGet();
			int ackNum = ackNumber.get();
			if (failNum >= RaceConfig.ThresholdFail && failNum != 0 && ((ackNum / failNum) <= 5)) {
				logger.info(RaceConfig.LogTracker + "ZY spout enables flowControl.");
				flowControl = true;
				ackNumber.set(0);
				failNumber.set(0);
			}
		}
		// 说明是pay的信息
		if (msgId instanceof MetaTuple) {
			MetaTuple tuple = (MetaTuple) values.get(1);
			tuple.incrFailTimes();
			// 重发次数大于MAX(默认5次)则抛弃
			if (tuple.getFailTimes() <= MetaTuple.MAX_FAIL_TIMES) {
				// 加入队列末尾，延迟一段时间
				try {
					emitQueue.put(tuple);
				} catch (InterruptedException e) {
					logger.info(RaceConfig.LogTracker + "ZY spout pay wait for re-emit interrupt:" + e.getMessage(), e);
				}

			} else {
//				logger.warn(RaceConfig.LogTracker + "ZY spout payMessage failed more than 5 times,payMsg:"
//						+ tuple.getMessage());
			}
		} else {
			// 订单信息直接丢弃，因为可能是重复的订单信息
			// spoutCollector.emit(values, msgId);
		}
	}

	@Override
	public void ack(Object msgId, List<Object> values) {
		// 在flowControl下，如果ack数量累积到10w(10s预估的偏移量)并且ack和fail比值大于20，则取消flowControl
		if (flowControl) {
			int ackNum = ackNumber.incrementAndGet();
			int failNum = failNumber.get();
			if (ackNum >= RaceConfig.ThresholdACK && failNum != 0 && ((ackNum / failNum) >= 20)) {
				logger.info(RaceConfig.LogTracker + "ZY spout disables flowControl.");
				flowControl = false;
				ackNumber.set(0);
				failNumber.set(0);
			}
		}

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
