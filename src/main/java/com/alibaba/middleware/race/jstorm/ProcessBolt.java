package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.MetaTuple;
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

	//private Logger logger = LoggerFactory.getLogger(ProcessBolt.class);

	// 如果这个ProcessBolt是单线程的话 可以考虑换成普通的HashMap
	private ConcurrentHashMap<Long, OrderMessage> taobaoOrderMap;
	private ConcurrentHashMap<Long, OrderMessage> tmallOrderMap;
	private transient OutputCollector collector;


	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		taobaoOrderMap = new ConcurrentHashMap<Long, OrderMessage>(RaceConfig.processOrderMapInitSegments, 0.75f,
				RaceConfig.processMapEntryArraySize);
		tmallOrderMap = new ConcurrentHashMap<Long, OrderMessage>(RaceConfig.processOrderMapInitSegments, 0.75f,
				RaceConfig.processMapEntryArraySize);


		//logger.info(RaceConfig.LogTracker + "ZY processBolt init finished.");
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String topicIdentifier = input.getString(0);
		Object message = input.getValue(1);
		
		switch (topicIdentifier) {
		// 当处理pay订单的时候，如果此时pay订单找不到taobao或者tmall的orderId，则默认为fail(虽然确实是成功了)
		// 然后超时fail堆积产生flowControl效果
		case RaceConfig.PayIdentifier:
			MetaTuple tuple = (MetaTuple) message;
			PaymentMessage payMessage = tuple.getMessage();
			Long orderId = payMessage.getOrderId();
			double price = payMessage.getPayAmount();

			// 如果这个订单还没有被发送用于计算ratio 则发送，防止重复计算的问题(这个消息不ack)
			if (tuple.getIsSendForRatio() == false) {
				tuple.setIsSendForRatio(true);
				sendMessage(RaceConfig.PayIdentifier, payMessage);
			}

			OrderMessage orderMessage = taobaoOrderMap.get(orderId);
			// taobao订单
			if (orderMessage != null) {
				// 将reducePrice和isZero方法设置为synchronized 防止同一个对象的竞争条件
				orderMessage.reducePrice(price);
				if (orderMessage.isZero()) {
					taobaoOrderMap.remove(orderId);
				}
				// taobao付款消息
//				logger.info(RaceConfig.LogTracker + "ZY processBolt retrieve taobaoOrder,identifier:" + topicIdentifier
//						+ ",key" + orderId);
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

//					logger.info(RaceConfig.LogTracker + "ZY processBolt retrieve tmallOrder,identifier:"
//							+ topicIdentifier + ",key" + orderId);
					// tmall付款消息
					sendMessage(input, RaceConfig.TmallIdentifier, payMessage);
					collector.ack(input);
				} else {
					// 没找到直接fail
//					logger.warn(RaceConfig.LogTracker + "ZY processBolt payMessage not found:" + orderId);
					collector.fail(input);
				}
			}

			break;
		case RaceConfig.TmallIdentifier:
//			logger.info(RaceConfig.LogTracker + "ZY processBolt get tmallOrder,identifier:" + topicIdentifier + ",key"
//					+ ((OrderMessage) message).getOrderId());

			// 需要有消息的去重过程 此处考虑到完全相同的消息不太可能同时到来 所以没有用锁
			OrderMessage tmallOrder = (OrderMessage) message;
			if (!tmallOrderMap.containsKey(tmallOrder.getOrderId())) {
				tmallOrderMap.put(tmallOrder.getOrderId(), tmallOrder);
				collector.ack(input);
			} else {
//				logger.warn(RaceConfig.LogTracker + "ZY processBolg get repeat tmallOder,identifier:"
//						+ tmallOrder.getOrderId());
				collector.fail(input);
			}

			break;
		case RaceConfig.TaobaoIdentifier:
//			logger.info(RaceConfig.LogTracker + "ZY processBolt get taobaoOrder,identifier:" + topicIdentifier + ",key"
//					+ ((OrderMessage) message).getOrderId());

			OrderMessage taobaoOrder = (OrderMessage) message;
			if (!taobaoOrderMap.containsKey(taobaoOrder.getOrderId())) {
				taobaoOrderMap.put(taobaoOrder.getOrderId(), taobaoOrder);
				collector.ack(input);
			} else {
//				logger.warn(RaceConfig.LogTracker + "ZY processBolg get repeat taobaoOrder,identifier:"
//						+ taobaoOrder.getOrderId());
				collector.fail(input);
			}

			break;
		default:
			//logger.error(RaceConfig.LogTracker + "ZY processBolt unrecognized Identifier:" + topicIdentifier
			//		+ ",message:" + message);
			break;
		}

	}

	private void sendMessage(Tuple tuple, String orderPaymentIdentifier, PaymentMessage payMessage) {
		List<Object> values = new Values(orderPaymentIdentifier, payMessage);
		collector.emit(tuple, values);
	}

	private void sendMessage(String orderPaymentIdentifier, PaymentMessage payMessage) {
		List<Object> values = new Values(orderPaymentIdentifier, payMessage);
		collector.emit(values);
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
