For all the scenarios played one message in every second. Total 1000 messages are played . Recovery service is running every minute.

# Scenario 1
There are two active MQ connections both are active. 

First ActiveMQ queue
![GitHub Logo](./images/FirstActiveMQBeforeTest.png)

Second ActiveMQ queue
![GitHub Logo](./images/SecondActiveMQBeforeTest.png)

Played 1000 messages, now
First ActiveMQ queue
![GitHub Logo](./images/SecondActiveMQAfterScenario1.png)

Second ActiveMQ queue
![GitHub Logo](./images/FristActiveMQAfterScenario1.png)
#### messages are equally distributed in both the queues (500,500)


# Scenario 2
First activeMQ connection is not active from the beginning of the test, only second one is active. Kept first one was stopped througout the remaining part of the test.

Second activeMQ queue before test
![GitHub Logo](./images/SecondAMQScenario2.png)

Second activeMQ queue after test
![GitHub Logo](./images/SecondActiveMQScenario2End.png)

Second queue consumed all 1000 messages.

# Scenario 3
Both the connections are active at the beginning of the test. Stopped first connection in the middle of the test.  First one was stopped througout the remaining part of the test.

First activeMQ queue before test
![GitHub Logo](./images/FirstActiveMQueueBeforeScenario3.png)


Second activeMQ queue before test
![GitHub Logo](./images/SecondActiveMQueueBeforeScenario3.png)

First activeMQ queue after test
![GitHub Logo](./images/FirstActiveMQueueAfterScenario3.png)

Second activeMQ queue after test
![GitHub Logo](./images/SecondActiveMQueueAfterScenario3.png)

Total number of messages consumed is 1000.
Messages consumed by the first queue is 74 (574 - 500)
Messages consumed by the second queue is 926


# Scenario 4
Till now whatever test we are doing, recover service is not tested. This one is for recovery, from the beginning first active MQ connection will be down. Somepoint of time during the test, it will be connected to check whether it is recovered and message delivered in the corresponding queue.

All the queues are purged and deleted.

After test number of messages in the first queue.
![GitHub Logo](./images/FirstQueueScenario4.png)

After test number of messages in the second queue.
![GitHub Logo](./images/SecondQueueScenario4.png)

What happens initially both the queues consumed messages. After consuming 19 messages, first activme was stopped. After a while it started again. Recovery service recovered the first connection. After that it consumed 412 messages.
total messages consumed by the first queue = 431
total messages consumed by the second queue = 569
total number of messages = 1000


# Scenario 5
Both the connection are stopped at beginning. Then they are started one by one. Recovery service should be able to connect both of them after they have been started.

Both the queues are deleted before test.

After test, queue 1
![GitHub Logo](./images/FirstQueueScenario5.png)
After test, queue 2
![GitHub Logo](./images/SecondQueueScenario5.png)

First queue consumed 435 messages. Second queue consumed 436 messages. There are 129 messages persisted in files. 
435+436+129=1000 messages.





