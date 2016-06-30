package com.alibaba.middleware.race.model;

public class MetaTuple {
	private String topic;
	private Object message;
	
	public MetaTuple(String topic, Object message) {
		this.topic = topic;
		this.message = message;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Object getMessage() {
		return message;
	}

	public void setMessage(Object message) {
		this.message = message;
	}
	
	
	
}
