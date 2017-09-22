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
First activeMQ connection is not active, only second one is active.

Second activeMQ queue before test
![GitHub Logo](./images/SecondAMQScenario2.png)

Second activeMQ queue after test
![GitHub Logo](./images/SecondActiveMQScenario2End.png)
