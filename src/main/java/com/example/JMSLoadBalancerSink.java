package com.example;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

import org.apache.activemq.ActiveMQConnection;
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
	private long messasgeCount = 0;

	// TODO: This will run recovery service, if dead connection becomes
	// available add them into the list.
	private ScheduledThreadPoolExecutor recoveryService = new ScheduledThreadPoolExecutor(1);

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
	private long retryDelay = 1500;
	// TODO: introduced new list to keep track of dead connection to retry.
	private List<Integer> injuryList = new ArrayList<>();

	/**
	 * To initialize ActiveMQ connections,
	 * 
	 * @param queues
	 *            - queue names, if there are multiple queues, each queue name
	 *            will be separated by "|".
	 * @param brokerURLs
	 *            - broker URL, if there are multiple brokers, each broker will
	 *            be separated by "|".
	 * @param retryConnectionCount
	 *            - how many times it will retry to establish a connection?
	 * @param retryMessageCount
	 *            - how many times it will retry to
	 * @param writeToFile
	 *            - whether write to file, if not able to send the message?
	 * @param recordsPerFile
	 *            - in case it writes to file, how many records it will write to
	 *            file?
	 * @param outputFileDirectory
	 *            - what will be the output directory to write the file?
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

		// Configure recovery service
		Recoverer recoverer = new Recoverer();
		recoveryService.scheduleWithFixedDelay(recoverer, 0, 1, TimeUnit.MINUTES);

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

					// Adding connection for first time.
					connectionList.add(connection);
					connection.start();

					// First time session creation
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
						injuryList.add(i);

						// Later when this connection is available we will set
						// it.
						// To fix the issue #1
						destinationsList.add(null);
						connectionList.add(null);
						sessionList.add(null);
						producerSinksList.add(null);
					}
				}
			}

			if (isConnected) {
				destinationsList.add(sessionList.get(i).createQueue(queueNamesList.get(i)));
				producerSinksList.add(sessionList.get(i).createProducer(destinationsList.get(i)));
				producerSinksList.get(i).setDeliveryMode(DeliveryMode.PERSISTENT);
			}
		}

		if (connectionList.size() == 0) {
			logger.log(java.util.logging.Level.WARNING, "There is no available connection(s), will try again later");
		}
	}

	/**
	 * 
	 * @param message
	 *            to send.
	 * @param messageType
	 *            type of the message, currently there are two types only "text"
	 *            and "map" message.
	 * @throws Exception
	 */
	public void process(String message, String messageType) throws Exception {
		TextMessage textMessage = null;
		MapMessage mapMessage = null;
		boolean isValidSession = false;
		if (numberOfSinks != 0 && producerSinksList.size() != 0 && connectionList.size() != 0) {
			boolean messageSent = false;

			try {
				try {
					if (sessionList.get(sinkIndex) != null && producerSinksList.get(sinkIndex) != null) {
						isValidSession = true;
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
						logger.log(java.util.logging.Level.INFO,
								messasgeCount + ". Sent message to  Sink No: " + sinkIndex);
						messasgeCount++;
					}

				} catch (Exception ex) {
					logger.log(java.util.logging.Level.WARNING, "Messaging failed to queue..");
				}

				// Message sending failed on first attempt

				// To fix issue #3
				// To try sending message across other available sinks.
				// We already tried this sink, have to check remaining sinks.
				int startingSinkNo = sinkIndex;
				while (!messageSent) {

					isValidSession = false;

					if (!injuryList.contains(sinkIndex)) {
						injuryList.add(sinkIndex);

						producerSinksList.set(sinkIndex, null);
						sessionList.set(sinkIndex, null);
						connectionList.set(sinkIndex, null);
						connectionFactoryList.set(sinkIndex, null);

						logger.log(java.util.logging.Level.WARNING, "Added to dead index list, sink No: " + sinkIndex
								+ ", Sink: " + (queueNamesList.get(sinkIndex).toString()));
					}

					numberOfSinks = producerSinksList.size();
					sinkIndex++;
					sinkIndex = (sinkIndex >= numberOfSinks) ? 0 : sinkIndex;

					if ((sinkIndex == startingSinkNo)) {
						if (writeToFile == true) {
							fileRecordCounter = writeMessageToFile(fileRecordCounter, message);
							if (fileRecordCounter >= recordsPerFile) {
								writer.close();
								fileRecordCounter = 0;
							}
							// TODO: why this is set to true? Not able to send
							// message yet, no possible way to send. to get rid off while loop
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
							if (sessionList.get(sinkIndex) != null) {
								textMessage = sessionList.get(sinkIndex).createTextMessage(message);
								messageSent = sendMessage(textMessage, producerSinksList.get(sinkIndex),
										numberOfTimesToTryToSendMessage);
								logger.log(java.util.logging.Level.INFO,
										messasgeCount + ". Sent message to  Sink No: " + sinkIndex);
								messasgeCount++;
							} else if (isValidSession) {
								// reconnectNSendMessage(message, messageType);
							} else {
								// try all the sinks, no luck.
								// get freedom from while true loop
//								break;
							}
						} catch (Exception ex) {
							logger.log(java.util.logging.Level.WARNING, "Messaging failed to queue..");
						}
					}
				}
				// Next time it will send to the next available sink.
				sinkIndex++;
				sinkIndex = (sinkIndex >= numberOfSinks) ? 0 : sinkIndex;

			} catch (Exception e) {
				// TODO: never give up
				// Try to reconnect
				if (isValidSession) {
					// reconnectNSendMessage(message, messageType);
				}

				logger.log(java.util.logging.Level.WARNING, "Issue sending message: " + e);
				e.printStackTrace();
			}
		} else if (writeToFile)

		{

			fileRecordCounter = writeMessageToFile(fileRecordCounter, message);
			if (fileRecordCounter >= recordsPerFile) {
				writer.close();
				fileRecordCounter = 0;
			}
			if (isValidSession) {
				// reconnectNSendMessage(message, messageType);
			}
		} else {
			// TODO : Code review
			if (isValidSession) {
				// reconnectNSendMessage(message, messageType);
			}
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
	private void reconnectNSendMessage(String message, String messageType) throws Exception {

		reconnect();

		process(message, messageType);

	}

	private void reconnect() throws InterruptedException {
		int numberOfDeadIndex = injuryList.size();

		for (int i = numberOfDeadIndex - 1; i > -1; i--) {
			connectionFactoryList.set(i, new ActiveMQConnectionFactory(brokerURL[i]));
			queueNamesList.set(i, queueNames[i]);

			for (int j = 0; j < numberOfTimesToTryToReconnect; j++) {
				try {
					ActiveMQConnectionFactory connectionFactory = connectionFactoryList.get(i);
					if (connectionFactory == null) {
						connectionFactory = new ActiveMQConnectionFactory(brokerURL[i]);
					}
					if (connectionFactory != null) {

						// Fix for issue #2
						connectionFactoryList.set(i, connectionFactory);
						Connection connection = connectionFactory.createConnection();

						// Starting connection. Trying to tack exceptions
						// related to
						// JMS
						ExceptionListener listener = new ExceptionHandler();
						connection.setExceptionListener(listener);
						connectionList.set(i, connection);
						connection.start();

						// Session creation, dursing reconnection
						Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
						sessionList.set(i, session);

						injuryList.remove(i);
						logger.log(java.util.logging.Level.INFO, " Able to establish connection " + i);
						MessageProducer producer = session.createProducer(destinationsList.get(i));
						producer.setDeliveryMode(DeliveryMode.PERSISTENT);
						producerSinksList.set(i, producer);
						destinationsList.set(i, sessionList.get(i).createQueue(queueNamesList.get(i)));

						break;
					}

				} catch (Exception ex) {
					logger.log(java.util.logging.Level.SEVERE, "Not able to establish connection :" + ex.getMessage(),
							ex);
					logger.log(java.util.logging.Level.WARNING, "Number of times failed to reconnect: " + (j + 1));
					// TODO : introduce retry delay
					Thread.sleep(retryDelay);
					if (j == numberOfTimesToTryToReconnect - 1) {
						// TODO: all attempts failed now adding into
						// deadIndexList
						// later we will use it to reconnect.
						if (!injuryList.contains(i)) {
							injuryList.add(i);
						}
					}
				}
			}

			if (connectionList.size() == 0) {
				logger.log(java.util.logging.Level.WARNING,
						"Failed to connect " + numberOfTimesToTryToReconnect + " times.");
			} else {
				numberOfSinks = producerSinksList.size();
			}
			logger.log(java.util.logging.Level.INFO,
					"Number of sinks available " + numberOfSinks + " number of dead sinks :" + injuryList.size());
		}
	}

	// Method to send the message on a queue.
	/**
	 * To send message
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

	/**
	 * Write the messages in a file.
	 * 
	 * @param fileRecordCounter
	 *            - number of records exists in the file.
	 * @param message
	 *            - message to be written.
	 * @return - total number of records wrote in the file.
	 */
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

	class Recoverer implements Runnable {

		@Override
		public void run() {
			try {
				logger.log(java.util.logging.Level.INFO,
						"Recovery service is running. Number of unavailable connections " + injuryList.size());
				reconnect();
			} catch (Throwable th) {
				logger.log(java.util.logging.Level.SEVERE, "ERROR occurred while trying to reconnect", th);
			}

		}

	}

}
