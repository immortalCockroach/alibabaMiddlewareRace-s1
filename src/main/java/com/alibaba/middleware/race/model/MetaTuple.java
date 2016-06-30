package com.alibaba.middleware.race.model;

/**
 * 为了将3个不同的消息统一封装进入阻塞队列而定义的封装结构
 * @author 打不死的小强
 *
 */
public class MetaTuple {
	private String topicIdentifier;
	private Object message;

	public MetaTuple(String topicIdentifier, Object message) {
		this.topicIdentifier = topicIdentifier;
		this.message = message;
	}

	public String getTopicIdentifier() {
		return topicIdentifier;
	}

	public void setTopic(String topicIdentifier) {
		this.topicIdentifier = topicIdentifier;
	}

	public Object getMessage() {
		return message;
	}

	public void setMessage(Object message) {
		this.message = message;
	}

}
