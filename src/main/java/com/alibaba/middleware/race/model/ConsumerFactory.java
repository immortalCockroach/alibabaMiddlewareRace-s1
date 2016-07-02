package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class ConsumerFactory {
	public static synchronized DefaultMQPushConsumer mkPushConsumerInstance(MessageListenerConcurrently listener) throws MQClientException {
		DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(RaceConfig.RocketMQGroup);
		
		// 系统自动设置,提交的时候去掉
		 //pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

		// 正式提交的时候去掉这行
		//pushConsumer.setNamesrvAddr("10.109.247.167:9876");
		// consumer.start();
		pushConsumer.subscribe(RaceConfig.MqPayTopic, "*");
		pushConsumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
		pushConsumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");

		pushConsumer.setConsumeMessageBatchMaxSize(16);
		pushConsumer.setPullBatchSize(64);
		
		pushConsumer.registerMessageListener(listener);
		pushConsumer.start();
		
		return pushConsumer;
	}
}
