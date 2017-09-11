package com.example;

import java.util.logging.Logger;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

public class ExceptionHandler implements ExceptionListener {

	private static final Logger LOGGER = Logger.getLogger("ExceptionHandler");

	@Override
	public void onException(JMSException exception) {

		LOGGER.log(java.util.logging.Level.WARNING, "JMSException : " + exception.getMessage(), exception);
	}

}
