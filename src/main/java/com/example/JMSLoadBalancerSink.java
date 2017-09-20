package com.example;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class JMSLoadBalancerSink {

	// Total number of sinks.
	private int numberOfSinks;
	// 
	private int numberOfTimesToTryToSendMessage;
	private int numberOfTimesToTryToReconnect;
	private String[] brokerURL;
	private String[] queueNames;
	private int sinkIndex = 0;
	private String outputFileDir;
	private int recordsPerFile;

	private List<ActiveMQConnectionFactory> connectionFactoryList = new ArrayList<ActiveMQConnectionFactory>();
	private List<Connection> connectionList = new ArrayList<Connection>();
	private List<Session> sessionList = new ArrayList<Session>();
	private List<String> queueNamesList = new ArrayList<String>();
	private List<Destination> destinationsList = new ArrayList<Destination>();
	private List<MessageProducer> producerSinksList = new ArrayList<MessageProducer>();

	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyyyy_hhmmss");
	private PrintWriter writer = null;
	private String strDate;
	private int fileRecordCounter = 0;
	private boolean writeToFile = true;

	protected Logger logger = Logger.getLogger("JMSLoadBalancerSink.log");
	// TODO : introduced new variable (needs review)
	private long retryDelay = 15000;
	// TODO: introduced new list to keep track of dead connection to retry.
	private List<Integer> deadIndexList = new ArrayList<>();

	/**
	 * To initialize ActiveMQ connections, 
	 * @param queues - queue names, if there are multiple queues, each queue name will be separated by "|".
	 * @param brokerURLs - broker URL, if there are multiple brokers, each broker will be separated by "|".
	 * @param retryConnectionCount - how many times it will retry to establish a connection?
	 * @param retryMessageCount - how many times it will retry to 
	 * @param writeToFile - whether write to file, if not able to send the message?
	 * @param recordsPerFile - in case it writes to file, how many records it will write to file?
	 * @param outputFileDirectory - what will be the output directory to write the file?
	 * @throws Exception 
	 */
	public synchronized void initialize(String queues, String brokerURLs, int retryConnectionCount,
			int retryMessageCount, boolean writeToFile, int recordsPerFile, String outputFileDirectory)
			throws Exception {

		this.numberOfTimesToTryToSendMessage = retryMessageCount;
		this.numberOfTimesToTryToReconnect = retryConnectionCount;
		this.outputFileDir = outputFileDirectory;
		this.recordsPerFile = recordsPerFile;
		this.writeToFile = writeToFile;
		this.queueNames = queues.split("\\|");
		this.brokerURL = brokerURLs.split("\\|");
		this.numberOfSinks = queueNames.length;

		if (queueNames.length != brokerURL.length) {
			numberOfSinks = (queueNames.length < brokerURL.length) ? queueNames.length : brokerURL.length;
			logger.log(java.util.logging.Level.WARNING,
					"The number of queue names and the number of server URL's are not equal. Number of queues and sinks used: "
							+ numberOfSinks);
		}

		connect();
	}

	/**
	 * Connect with brokers. If they are not available then send them to
	 * deadIndexList. To reconnect later.
	 * 
	 * @throws InterruptedException
	 * @throws Exception
	 * @throws JMSException
	 */
	private void connect() throws InterruptedException, Exception, JMSException {
		for (int i = 0; i < numberOfSinks; i++) {
			connectionFactoryList.add(new ActiveMQConnectionFactory(brokerURL[i]));
			queueNamesList.add(queueNames[i]);
			boolean isConnected = false;

			for (int j = 0; j < numberOfTimesToTryToReconnect; j++) {
				try {
					Connection connection = connectionFactoryList.get(i).createConnection();

					// Starting connection. Trying to tack exceptions related to
					// JMS
					ExceptionListener listener = new ExceptionHandler();
					connection.setExceptionListener(listener);
					connectionList.add(connection);
					connection.start();

					// Session creation
					sessionList.add(connectionList.get(i).createSession(false, Session.AUTO_ACKNOWLEDGE));
					isConnected = true;
					break;
				} catch (Exception ex) {
					logger.log(java.util.logging.Level.WARNING, "Number of times failed to connect: " + (j + 1));
					// TODO : introduced retry delay
					Thread.sleep(retryDelay);
					if (j == numberOfTimesToTryToReconnect - 1) {
						// TODO: all attempts failed now adding into
						// deadIndexList
						// later we will use it to reconnect.
						deadIndexList.add(i);
					}
				}
			}

			if (connectionList.size() == 0) {
				logger.log(java.util.logging.Level.WARNING,
						"Failed to connect " + numberOfTimesToTryToReconnect + " times.");
				throw new Exception("None of the Messaging queues are accessible.");
			} else if (isConnected) {
				destinationsList.add(sessionList.get(i).createQueue(queueNamesList.get(i)));
				producerSinksList.add(sessionList.get(i).createProducer(destinationsList.get(i)));
				producerSinksList.get(i).setDeliveryMode(DeliveryMode.PERSISTENT);
			}
		}
	}

	/**
	 * 
	 * @param message to send.
	 * @param messageType type of the message, currently there are two types only "text" and "map" message.
	 * @throws Exception
	 */
	public void process(String message, String messageType) throws Exception {
		TextMessage textMessage = null;
		MapMessage mapMessage = null;
		if (numberOfSinks != 0 && producerSinksList.size() != 0 && connectionList.size() != 0) {
			boolean messageSent = false;
			try {
				try {
					if (messageType.equalsIgnoreCase("text")) {
						textMessage = sessionList.get(sinkIndex).createTextMessage(message);
						messageSent = sendMessage(textMessage, producerSinksList.get(sinkIndex),
								numberOfTimesToTryToSendMessage);
					} else {
						mapMessage = sessionList.get(sinkIndex).createMapMessage();
						mapMessage.setString("message", message);
						messageSent = sendMessage(mapMessage, producerSinksList.get(sinkIndex),
								numberOfTimesToTryToSendMessage);
					}
				} catch (Exception ex) {
					logger.log(java.util.logging.Level.WARNING, "Messaging failed to queue..");
				}

				while (!messageSent) {
					logger.log(java.util.logging.Level.WARNING,
							"Removing Sink No: " + sinkIndex + ", Sink: " + (queueNamesList.get(sinkIndex).toString()));
					try {
						producerSinksList.remove(sinkIndex);
						sessionList.remove(sinkIndex);
						// TODO: needs a code review
						// added those index to
						connectionList.remove(sinkIndex);
						connectionFactoryList.remove(sinkIndex);
						queueNamesList.remove(sinkIndex);
						if (!deadIndexList.contains(sinkIndex)) {
							deadIndexList.add(sinkIndex);
						}
					} catch (IndexOutOfBoundsException ie) {
					}

					numberOfSinks = producerSinksList.size();
					sinkIndex = (sinkIndex >= numberOfSinks) ? 0 : sinkIndex;

					if ((numberOfSinks == 0 || producerSinksList.size() == 0)) {
						if (writeToFile == true) {
							fileRecordCounter = writeMessageToFile(fileRecordCounter, message);
							if (fileRecordCounter >= recordsPerFile) {
								writer.close();
								fileRecordCounter = 0;
							}
							// TODO: why this is set to true? Not able to send
							// message yet?
							messageSent = true;
						} else {
							// TODO : code review
							// throw new Exception("None of the queues are
							// reachable and writeToFile is: " + writeToFile);
							logger.log(java.util.logging.Level.WARNING,
									"None of the queues are reachable and writeToFile is: " + writeToFile);
						}
					} else {
						try {
							textMessage = sessionList.get(sinkIndex).createTextMessage(message);
							messageSent = sendMessage(textMessage, producerSinksList.get(sinkIndex),
									numberOfTimesToTryToSendMessage);
							reconnect(message, messageType);
						} catch (Exception ex) {
							logger.log(java.util.logging.Level.WARNING, "Messaging failed to queue..");
						}
					}
				}
				sinkIndex++;
				sinkIndex = (sinkIndex >= numberOfSinks) ? 0 : sinkIndex;
			} catch (Exception e) {
				// TODO: never give up
				// Try to reconnect
				reconnect(message, messageType);

				logger.log(java.util.logging.Level.WARNING, "Issue sending message: " + e);
				e.printStackTrace();
			}
		} else if (writeToFile) {

			fileRecordCounter = writeMessageToFile(fileRecordCounter, message);
			if (fileRecordCounter >= recordsPerFile) {
				writer.close();
				fileRecordCounter = 0;
			}
			reconnect(message, messageType);
		} else {
			// TODO : Code review
			reconnect(message, messageType);
			// throw new Exception("None of the queues are reachable and
			// writeToFile is: " + writeToFile);
		}
	}

	/**
	 * Try to reconnect and send message.
	 * 
	 * @param message
	 * @param messageType
	 * @throws Exception
	 */
	private void reconnect(String message, String messageType) throws Exception {

		int numberOfDeadIndex = deadIndexList.size();
		for (int i = numberOfDeadIndex - 1; i > -1; i--) {
			connectionFactoryList.add(new ActiveMQConnectionFactory(brokerURL[i]));
			queueNamesList.add(queueNames[i]);

			for (int j = 0; j < numberOfTimesToTryToReconnect; j++) {
				try {
					Connection connection = connectionFactoryList.get(i).createConnection();

					// Starting connection. Trying to tack exceptions related to
					// JMS
					ExceptionListener listener = new ExceptionHandler();
					connection.setExceptionListener(listener);
					connectionList.add(connection);
					connection.start();

					// Session creation
					sessionList.add(connectionList.get(i).createSession(false, Session.AUTO_ACKNOWLEDGE));
					deadIndexList.remove(i);

					break;
				} catch (Exception ex) {
					logger.log(java.util.logging.Level.WARNING, "Number of times failed to reconnect: " + (j + 1));
					// TODO : introduce retry delay
					Thread.sleep(retryDelay);
					if (j == numberOfTimesToTryToReconnect - 1) {
						// TODO: all attempts failed now adding into
						// deadIndexList
						// later we will use it to reconnect.
						if (!deadIndexList.contains(i)) {
							deadIndexList.add(i);
						}
					}
				}
			}

			if (connectionList.size() == 0) {
				logger.log(java.util.logging.Level.WARNING,
						"Failed to connect " + numberOfTimesToTryToReconnect + " times.");
				// throw new Exception("None of the Messaging queues are
				// accessible.");
			} else {
				// Some bug here, java.lang.IndexOutOfBoundsException: Index: 1,
				// Size: 1
				// destinationsList.add(sessionList.get(i).createQueue(queueNamesList.get(i)));
				// producerSinksList.add(sessionList.get(i).createProducer(destinationsList.get(i)));
				// producerSinksList.get(i).setDeliveryMode(DeliveryMode.PERSISTENT);
				numberOfSinks = producerSinksList.size();
			}
		}

		process(message, messageType);

	}

	// Method to send the message on a queue.
	/**
	 * 
	 * @param message
	 *            to be sent.
	 * @param producer
	 *            message producer.
	 * @param numberOfTimesToTryToSendMessage
	 * @return
	 */
	public boolean sendMessage(TextMessage message, MessageProducer producer, int numberOfTimesToTryToSendMessage) {
		boolean messageSent = false;
		for (int i = 0; i < numberOfTimesToTryToSendMessage; i++) {
			try {
				producer.send(message);
				messageSent = true;
				break;
			} catch (JMSException ex) {
				logger.log(java.util.logging.Level.WARNING, "Number of times failed to send the message: " + (i + 1));
			}
		}
		return messageSent;
	}

	/**
	 * 
	 * @param message
	 * @param producer
	 * @param numberOfTimesToTryToSendMessage
	 * @return
	 */
	public boolean sendMessage(MapMessage message, MessageProducer producer, int numberOfTimesToTryToSendMessage) {

		logger.log(java.util.logging.Level.INFO, "Recieved Map Message : " + message);
		boolean messageSent = false;

		for (int i = 0; i < numberOfTimesToTryToSendMessage; i++) {
			try {
				producer.send(message);
				messageSent = true;
				break;
			} catch (JMSException ex) {
				logger.log(java.util.logging.Level.WARNING, "Number of times failed to send the message: " + (i + 1));
			}
		}
		logger.log(java.util.logging.Level.INFO, "Message Sent Flag : " + messageSent);
		return messageSent;
	}

	// Method to write the messages to a file.
	public int writeMessageToFile(int fileRecordCounter, String message) {
		if (fileRecordCounter == 0) {
			strDate = simpleDateFormat.format(new Date());
			try {
				writer = new PrintWriter(outputFileDir + "MessagesFile" + strDate + ".txt");
			} catch (FileNotFoundException e) {
				logger.log(java.util.logging.Level.WARNING, "The output directory is not present.");
			}
		}

		writer.write(message + "\n");
		fileRecordCounter++;

		return fileRecordCounter;
	}


}
