# ActiveMqStaleConnectionHandler
How to handle reconnection after inactivity IO exception from ActiveMQ

# Approach

* Connctions - We maintain the connections in a list. As soon as any connection is not available, we assign null to it. Before using any connection we make sure that it is not null. We maintain a list of indexes which are not currently available. We call it injuryList. 

* Recovery Service - Recovery service is a ScheduledThreadPoolExecutor, with one thread. It monitors injuryList in a periodic interval. If any connection fron the injury list it reinitializes the connection and store it in the same index, so that application can use that connection.

# Reference
[Failover Transport Reference](http://activemq.apache.org/failover-transport-reference.html)

[Auto Reconnection](http://activemq.apache.org/how-can-i-support-auto-reconnection.html)

[Inactivity Monitoring](http://activemq.apache.org/activemq-inactivitymonitor.html)
