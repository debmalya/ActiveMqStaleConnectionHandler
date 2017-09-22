# ActiveMqStaleConnectionHandler
How to handle reconnection after inactivity IO exception from ActiveMQ

# Approach

* Connctions - We maintain the connections in a list. As soon as any connection is not available, we assign null to it. Before using any connection we make sure that it is not null. We maintain a list of indexes which are not currently available. We call it injuryList. 

* Recovery Service - Recovery service is a ScheduledThreadPoolExecutor, with one thread. It monitors injuryList in a periodic interval. If any connection from the injury list is available it initialized the connection and store it in the same index, so that application can use that connection.

For example, we have four porperly initialized connections, they are maintained in a list.
 -------
    0   
 -------
    1   
 -------
    2   
 -------
    3   
 -------

Now, if second connection goes down due to some reason, this list will become like this.

 -------
    0   
 -------
   null  
 -------
    2   
 -------
    3   
 -------

There will be an addition in the injury List
 -------
    1   
 -------

After some point of time, this connection becomes available and recovery service finds it is available in a periodic run. Freshly intializes the connection assign it in the proper index.

 -------
    0   
 -------
    1   
 -------
    2   
 -------
    3   
 -------
 
 Injury List is blank now.
 
Just imagine there is a sports team (e.g. Basketball, football), every player has their fixed jursey (in this case index). Whenever any player is injured (s)he is not available in the active player list. After a period, when (s)he is fit for the game (s)he is listed again in the starting team.


# Reference
[Failover Transport Reference](http://activemq.apache.org/failover-transport-reference.html)

[Auto Reconnection](http://activemq.apache.org/how-can-i-support-auto-reconnection.html)

[Inactivity Monitoring](http://activemq.apache.org/activemq-inactivitymonitor.html)
