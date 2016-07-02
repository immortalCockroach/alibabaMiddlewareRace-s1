package com.alibaba.middleware.race.model;

/**
 * Taobao和Tmall交易值的封装类，防止过多的装箱拆箱和put操作
 * 
 * @author immortalCockRoach
 *
 */
public class OrderTranValue {
	private double value;

	public OrderTranValue(double value) {
		super();
		this.value = value;
	}

	public double getValue() {
		return value;
	}

	public synchronized void incrValue(double incr) {
		this.value += incr;
	}

}
