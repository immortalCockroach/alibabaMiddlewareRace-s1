package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderTranValue;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.WPRatio;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class CalAndPersistBolt implements IRichBolt {

	private static final Logger logger = LoggerFactory.getLogger(CalAndPersistBolt.class);

	private DefaultTairManager tairClient;

	// 这里的map的key只涉及时间戳，防止过多的字符串拼接的开销
	private ConcurrentHashMap<Long, OrderTranValue> taobaoOrderTranMap;
	private ConcurrentHashMap<Long, OrderTranValue> tmallOrderTranMap;

	private ConcurrentHashMap<Long, WPRatio> wpRatioMap;
	
	private HashMap<Long,Double> lastTaobaoMap;
	private HashMap<Long,Double> lastTmallMap;
	private HashMap<Long,Double> lastRatioMap;

	private Lock taobaoMapLock;
	private Lock tmallLock;
	private Lock ratioLock;

	private int writeCount;

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub

		this.collector = collector;

		taobaoOrderTranMap = new ConcurrentHashMap<Long, OrderTranValue>();
		tmallOrderTranMap = new ConcurrentHashMap<Long, OrderTranValue>();
		wpRatioMap = new ConcurrentHashMap<Long, WPRatio>();
		
		lastTaobaoMap = new HashMap<Long,Double>();
		lastTmallMap = new HashMap<Long,Double>();
		lastRatioMap = new HashMap<Long,Double>();

		taobaoMapLock = new ReentrantLock();
		tmallLock = new ReentrantLock();
		ratioLock = new ReentrantLock();

		writeCount = 0;

		List<String> confServers = new ArrayList<String>();
		confServers.add(RaceConfig.TairConfigServer);
		confServers.add(RaceConfig.TairSalveConfigServer);

		tairClient = new DefaultTairManager();
		tairClient.setConfigServerList(confServers);

		tairClient.setGroupName(RaceConfig.TairGroup);

		tairClient.init();

		logger.info(RaceConfig.LogTracker + "ZY CalBolt init finished.");

		// 定时任务 30s开始 每30s间隔执行一次
		Timer t = new Timer();
		t.schedule(new TimerTask() {

			@Override
			public void run() {
				writeTaobao();
				writeTmall();
				writeRatio();
				writeCount++;

			}
		}, 30 * 1000, 60 * 1000);
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String identifier = input.getString(0);
		PaymentMessage payMessage = (PaymentMessage) input.getValue(1);

//		if(identifier.equals(RaceConfig.TaobaoIdentifier)){
//		logger.info(
//				RaceConfig.LogTracker + "ZY calbolt get payment,identifier:" + identifier + ",message:" + payMessage);
//		}
		Long timeKey = (payMessage.getCreateTime() / 1000 /60) * 60;
		double amount = payMessage.getPayAmount();

		// pay的话说明用于计算ratio 而且不ack
		switch (identifier) {
		case RaceConfig.PayIdentifier:
			if (!wpRatioMap.containsKey(timeKey)) {
				ratioLock.lock();

				boolean add = true;
				if (wpRatioMap.containsKey(timeKey)) {
					WPRatio ratio = wpRatioMap.get(timeKey);
					if (payMessage.getPayPlatform() == 0) {
						ratio.incrPcValue(amount);
					} else {
						ratio.incrWirelessValue(amount);
					}
					add = false;
					//logger.info(RaceConfig.LogTracker + "ZY CalBolt get conlicts add ratioMap,timeKey:" + timeKey);
				}
				try {
					if (add) {
						WPRatio ratio = new WPRatio();
						// pc
						if (payMessage.getPayPlatform() == 0) {
							ratio.incrPcValue(amount);
						} else {
							ratio.incrWirelessValue(amount);
						}
						wpRatioMap.put(timeKey, ratio);
					}
				} finally {
					ratioLock.unlock();
				}
			} else {
				WPRatio ratio = wpRatioMap.get(timeKey);
				if (payMessage.getPayPlatform() == 0) {
					ratio.incrPcValue(amount);
				} else {
					ratio.incrWirelessValue(amount);
				}
			}
			break;
		case RaceConfig.TaobaoIdentifier:
			// 10位的时间戳，此处不转成String 防止字符串拼接的开销

			// 当map中不存在key的时候 需要lock住map,否则多线程的put结果可能会覆盖
			if (!taobaoOrderTranMap.containsKey(timeKey)) {
				taobaoMapLock.lock();

				// 防止2个线程同时得到containKey为false时的冲突
				boolean add = true;
				if (taobaoOrderTranMap.containsKey(timeKey)) {
					taobaoOrderTranMap.get(timeKey).incrValue(amount);
					add = false;
					//logger.info(RaceConfig.LogTracker + "ZY CalBolt get conlicts add taobaoMap,timeKey:" + timeKey);
				}
				try {
					if (add) {
						OrderTranValue value = new OrderTranValue(amount);
						taobaoOrderTranMap.put(timeKey, value);
					}
				} finally {
					taobaoMapLock.unlock();
				}

			} else { // 这种情况下，由于更新字段是synchronized 所以不需要lock
				taobaoOrderTranMap.get(timeKey).incrValue(amount);
			}
			// taobao和tmall的paymessage需要ack
			collector.ack(input);
			break;
		case RaceConfig.TmallIdentifier:

				// 同上
				if (!tmallOrderTranMap.containsKey(timeKey)) {
					tmallLock.lock();

					boolean add = true;
					if (tmallOrderTranMap.containsKey(timeKey)) {
						tmallOrderTranMap.get(timeKey).incrValue(amount);
						add = false;
						//logger.info(RaceConfig.LogTracker + "ZY CalBolt get conlicts add tmallMap:" + timeKey);
					}
					try {
						if (add) {
							OrderTranValue value = new OrderTranValue(amount);
							tmallOrderTranMap.put(timeKey, value);
						}
					} finally {
						tmallLock.unlock();
					}

				} else { // 同上
					tmallOrderTranMap.get(timeKey).incrValue(amount);
				}
				// taobao和tmall的paymessage需要ack
				collector.ack(input);
			break;
		default:
			break;
		}
	}
	
	private static double round2(double value) {

		long factor = (long) 100;
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}

	private void writeTaobao() {
		for (Map.Entry<Long, OrderTranValue> entry : taobaoOrderTranMap.entrySet()) {
			// taobao的不用保留2位小数
			
			Long timeKey = entry.getKey();
			double value = entry.getValue().getValue();
			// 和上次基本没差
			if (lastTaobaoMap.containsKey(timeKey) && Math.abs(lastTaobaoMap.get(timeKey) - value) < 0.005) {
				continue;
			} else {
				lastTaobaoMap.put(timeKey, value);
				tairClient.put(RaceConfig.TairNamespace, RaceConfig.PrexTaobao + timeKey,
						value);
//				if (!code.isSuccess()) {
////					logger.error(RaceConfig.LogTracker + "ZY CalBolt put taobao error,code" + code.getCode() + ",message:"
////							+ code.getMessage() + ",key:" + entry.getKey());
//				} else {
////					logger.info(RaceConfig.LogTracker + "ZY CalBolt put taobao success,key:" + entry.getKey() + ",value:"
////							+ entry.getValue().getValue());
//				}
			}
			

		}
		//logger.info(RaceConfig.LogTracker + "ZY CalBolt put taobao time:" + writeCount);
	}

	private void writeTmall() {
		for (Map.Entry<Long, OrderTranValue> entry : tmallOrderTranMap.entrySet()) {
			// tmall的不用保留2位小数
			
			Long timeKey = entry.getKey();
			double value = entry.getValue().getValue();
			// 和上次基本没差
			if (lastTmallMap.containsKey(timeKey) && Math.abs(lastTmallMap.get(timeKey) - value) < 0.005) {
				continue;
			} else {
				lastTmallMap.put(timeKey, value);
				tairClient.put(RaceConfig.TairNamespace, RaceConfig.PrexTmall + timeKey,
						value);
//				if (!code.isSuccess()) {
//					// logger.error(RaceConfig.LogTracker + "ZY CalBolt put
//					// tmall error,code" + code.getCode() + ",message:"
//					// + code.getMessage() + ",data:" + entry.getKey());
//				} else {
//					// logger.info(RaceConfig.LogTracker + "ZY CalBolt put tmall
//					// success,key:" + entry.getKey() + ",value:"
//					// + entry.getValue().getValue());
//				}
			}
		}
		//logger.info(RaceConfig.LogTracker + "ZY CalBolt put tmall time:" + writeCount);
	}

	private void writeRatio() {
		List<Map.Entry<Long, WPRatio>> list = new ArrayList<Map.Entry<Long, WPRatio>>(wpRatioMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Long, WPRatio>>() {
			// 升序排序
			public int compare(Entry<Long, WPRatio> o1, Entry<Long, WPRatio> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}

		});

		double pcAddi = 0.0f;
		double wirelessAddi = 0.0f;
		for (Map.Entry<Long, WPRatio> mapping : list) {
			double pcValue = mapping.getValue().getPcValue() + pcAddi;
			double wireLessValue = mapping.getValue().getWirelessValue() + wirelessAddi;

			// 下一次迭代作准备
			pcAddi = pcValue;
			wirelessAddi = wireLessValue;

//			ResultCode code;
			
			Long timeKey = mapping.getKey();
			double value = 0f;
			if (Math.abs(pcValue) < 0.0001) {
				value = wireLessValue;
			} else {
				value = round2(wireLessValue / pcValue);
			}
			
			if (lastRatioMap.containsKey(timeKey) && Math.abs(lastRatioMap.get(timeKey) - value) < 0.005) {
				continue;
			} else {
				lastRatioMap.put(timeKey, value);
				tairClient.put(RaceConfig.TairNamespace, RaceConfig.PrexRatio + timeKey,
						value);
			}

			// ratio需要保留2位小数,顺便防止PC值为接近0的情况(虽然这个基本没可能)
//			if (Math.abs(pcValue) < 0.0001) {
//				
//			} else {
//				lastRatioMap.put(timeKey, value);
//				code = tairClient.put(RaceConfig.TairNamespace, RaceConfig.PrexRatio + timeKey,
//						round2(wireLessValue / pcValue));
//			}

//			if (!code.isSuccess()) {
////				logger.error(RaceConfig.LogTracker + "ZY CalBolt put ratio error,code" + code.getCode() + ",message:"
////						+ code.getMessage() + ",data:" + mapping.getKey());
//			} else {
////				logger.info(RaceConfig.LogTracker + "ZY CalBolt put ratio success,key:" + mapping.getKey() + ",value:"
////						+ wireLessValue + "/" + pcValue);
//			}
		}
		logger.info(RaceConfig.LogTracker + "ZY CalBolt put ratio time:" + writeCount);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// igonre

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
