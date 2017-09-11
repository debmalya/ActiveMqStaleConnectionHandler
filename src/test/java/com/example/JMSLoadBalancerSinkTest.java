package com.example;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JMSLoadBalancerSinkTest {

	static JMSLoadBalancerSink sinking = new JMSLoadBalancerSink();

	@BeforeClass
	public static void setup() {
		try {
			// Running ActiveMQ locally.
			sinking.initialize("test.reconnection|test.reconnection", "tcp://localhost:61616|tcp://192.168.165.134:61616", 3, 3, true, 10, "./output");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertFalse(e.getMessage(), true);
		}
	}

	/**
	 * 
	 */
	@Test
	public void testInitialize() {
		Assert.assertNotNull(sinking);

	}

	/**
	 * Just sending a message and checking whether successful or not.
	 */
	@Test
	public void testProcess() {
		try {
			sinking.process("This is a test message", "text");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertFalse(e.getMessage(), true);
		}
	}

	/**
	 * Trying 1000 times. In the middle manually stopped and started ACTIVEMQ to
	 * test reconnect.
	 * 
	 * Is it possible to do from the program ? 
	 * 
	 */
	@Test
	public void testProcessMultipleSending() {
		for (int i = 0; i < 1000; i++) {
			try {
				sinking.process("This is a test message", "text");
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
				Assert.assertFalse(e.getMessage(), true);
			}
		}
	}

}
