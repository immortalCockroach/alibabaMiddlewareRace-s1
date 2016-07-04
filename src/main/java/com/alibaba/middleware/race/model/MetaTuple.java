package com.alibaba.middleware.race.model;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 为了PaymentMessage的流量和重发控制而定义的封装结构,设置了最大重发次数5，超过5次丢弃
 * 
 * @author 打不死的小强
 *
 */
public class MetaTuple {
	private AtomicInteger failTimes;

	private PaymentMessage message;
	public static final int MAX_FAIL_TIMES = 5;
	// private CountDownLatch latch;

	private boolean isSendForRatio;

	// Kryo默认需要无参数构造函数
	public MetaTuple() {

	}

	@Override
	public String toString() {
		return "MetaTuplr:" + message.toString();
	}

	public MetaTuple(PaymentMessage message) {
		this.message = message;
		this.failTimes = new AtomicInteger(0);
		// latch = new CountDownLatch(1);
	}

	public boolean getIsSendForRatio() {
		return this.isSendForRatio;
	}

	public void setIsSendForRatio(boolean value) {
		this.isSendForRatio = value;
	}

	public PaymentMessage getMessage() {
		return message;
	}

	// public void waitForEmit() throws InterruptedException {
	// latch.await(1, TimeUnit.SECONDS);
	// }

	public int getFailTimes() {
		return failTimes.intValue();
	}

	public void incrFailTimes() {
		this.failTimes.incrementAndGet();
	}

	public void setMessage(PaymentMessage message) {
		this.message = message;
	}

}
