package com.alibaba.middleware.race.model;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 无线和PC交易值的封装类，和{@link OrderTranValue}类似，防止过多的拆箱、装箱和put
 * 
 * @author immortalCockRoach
 *
 */
public class WPRatio {
	private double pcValue;
	private double wirelessValue;

	// 2个增长互不干扰
	private Lock pcValueLock;
	private Lock wirelessValueLock;

	public WPRatio() {
		this.pcValueLock = new ReentrantLock();
		this.wirelessValueLock = new ReentrantLock();
	}

	public double getPcValue() {
		return pcValue;
	}

	public void incrPcValue(double pcValue) {
		pcValueLock.lock();
		this.pcValue += pcValue;
		pcValueLock.unlock();
	}

	public double getWirelessValue() {
		return wirelessValue;
	}

	public void incrWirelessValue(double wirelessValue) {
		wirelessValueLock.lock();
		this.wirelessValue += wirelessValue;
		wirelessValueLock.unlock();
	}

}
