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
#### messages are equally distributed in both the queues


# Scenario 2
First activeMQ connection is not active from the beginning of the test, only second one is active. Kept first one througout the remaining part of the test.

Second activeMQ queue before test
![GitHub Logo](./images/SecondAMQScenario2.png)

Second activeMQ queue after test
![GitHub Logo](./images/SecondActiveMQScenario2End.png)

# Scenario 3
Both the connections are active at the beginning of the test. Stopped first connection in the middle of the test. Kept first one througout the remaining part of the test.

First activeMQ queue before test
![GitHub Logo](./images/FirstActiveMQueueBeforeScenario3.png)

Second activeMQ queue before test
![GitHub Logo](./images/SecondActiveMQueueBeforeScenario3.png)

First activeMQ queue after test
![GitHub Logo](./images/FirstActiveMQueueAfterScenario3.png)

Second activeMQ queue after test
![GitHub Logo](./images/SecondActiveMQueueAfterScenario3.png)




