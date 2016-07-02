package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ProcessBolt implements IRichBolt {

	private Logger logger = LoggerFactory.getLogger(ProcessBolt.class);

	private ConcurrentHashMap<Long, OrderMessage> taobaoOrderMap;
	private ConcurrentHashMap<Long, OrderMessage> tmallOrderMap;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		taobaoOrderMap = new ConcurrentHashMap<Long, OrderMessage>(RaceConfig.processOrderMapInitSegments, 0.75f,
				RaceConfig.processMapEntryArraySize);
		tmallOrderMap = new ConcurrentHashMap<Long, OrderMessage>(RaceConfig.processOrderMapInitSegments, 0.75f,
				RaceConfig.processMapEntryArraySize);

		// Thread traverseThread = new Thread(new Runnable() {
		//
		// @Override
		// public void run() {
		// PaymentMessage payMessage = null;
		// while (true) {
		// try {
		// payMessage = payCacheQueue.take();
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// logger.info("ZY bolt new thread take operation interupt:" +
		// e.getMessage(), e);
		// }
		// if (payMessage == null) {
		// logger.warn("ZY bolt new thread take operation get null");
		// continue;
		// } else {
		// Long orderId = payMessage.getOrderId();
		// double price = payMessage.getPayAmount();
		// OrderMessage orderMessage = taobaoOrderMap.get(orderId);
		// // taobao订单
		// if (orderMessage != null) {
		// orderMessage.reducePrice(price);
		// if (orderMessage.isZero()) {
		// taobaoOrderMap.remove(orderId);
		// }
		// sendMessage(input, "2", payMessage);
		// } else {
		// // Tmall订单
		// orderMessage = tmallOrderMap.get(orderId);
		// if (orderMessage != null) {
		// orderMessage.reducePrice(price);
		// if (orderMessage.isZero()) {
		// tmallOrderMap.remove(orderId);
		// }
		// sendMessage(input, "2", payMessage);
		// } else {
		// // 没找到,再放进去
		// logger.info("ZY payMessage:" + orderId);
		// try {
		// payCacheQueue.put(payMessage);
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// logger.info("ZY bolt new thread put operation interupt:" +
		// e.getMessage(), e);
		// }
		// }
		// }
		// }
		// }
		// }
		// });
		// traverseThread.start();

		logger.info(RaceConfig.LogTracker + "ZY processBolt init finished.");
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String topicIdentifier = input.getString(0);
		Object message = input.getValue(1);

		if (message == null) {
			logger.error(RaceConfig.LogTracker + "ZY processBolt message is null:" + topicIdentifier);
			return;
		}
		switch (topicIdentifier) {
		// 当处理pay订单的时候，如果此时pay订单找不到taobao或者tmall的orderId，则默认为fail(虽然确实是成功了)
		// 然后超时fail堆积产生flowControl效果
		case RaceConfig.PayIdentifier:
			PaymentMessage payMessage = (PaymentMessage) message;
			Long orderId = payMessage.getOrderId();
			double price = payMessage.getPayAmount();
			OrderMessage orderMessage = taobaoOrderMap.get(orderId);
			// taobao订单
			if (orderMessage != null) {
				// 将reducePrice和isZero方法设置为synchronized 防止同一个对象的竞争条件
				orderMessage.reducePrice(price);
				if (orderMessage.isZero()) {
					taobaoOrderMap.remove(orderId);
				}
				// taobao付款消息
				sendMessage(input, RaceConfig.TaobaoIdentifier, payMessage);
				collector.ack(input);
			} else {
				// Tmall订单
				orderMessage = tmallOrderMap.get(orderId);
				if (orderMessage != null) {
					// 同上
					orderMessage.reducePrice(price);
					if (orderMessage.isZero()) {
						tmallOrderMap.remove(orderId);
					}
					// tmall付款消息
					sendMessage(input, RaceConfig.TmallIdentifier, payMessage);
					collector.ack(input);
				} else {
					// 没找到直接fail
					logger.warn(RaceConfig.LogTracker + "ZY processBolt payMessage not found:" + orderId);
					collector.fail(input);
				}
			}

			break;
		case RaceConfig.TmallIdentifier:
			tmallOrderMap.put(((OrderMessage) message).getOrderId(), (OrderMessage) message);
			collector.ack(input);
			break;
		case RaceConfig.TaobaoIdentifier:
			taobaoOrderMap.put(((OrderMessage) message).getOrderId(), (OrderMessage) message);
			collector.ack(input);
			break;
		default:
			logger.error(RaceConfig.LogTracker + "ZY processBolt unrecognized Identifier:" + topicIdentifier);
			break;
		}

	}

	private void sendMessage(Tuple tuple, String orderPaymentIdentifier, PaymentMessage payMessage) {
		List<Object> values = new Values(orderPaymentIdentifier, payMessage);
		collector.emit(tuple, values);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(RaceConfig.Topic, RaceConfig.Message));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
