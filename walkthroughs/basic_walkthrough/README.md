# Basic Kafka Walkthrough
This walkthrough deals with getting a complete kafka newbie familiar with some of the concepts around kafka.  
One thing to note is, this walkthrough is mainly targetted at Osx users, linux users can also follow this walkthrough but will need to substitute ``$(boot2docker ip)`` with ``localhost`` in all the commands.  


##  Pre-requisite
The reader should have followed the instructions outlayed in the [main readme](../../README.md) and should have a full 3 node kafka cluster up and running

## Topics
A kafka topic is made up of partitions, with each partition containing a sequence of messages that are sent to it.  
The partition that each message is sent to, is detemined by the message key. If a key isn't provided, then the default kafka producer will randomly pick a partition and send messages to it (for a default of 10 mins)  
If a key is provided then, the key will be hashed to determine a partition and the message will be sent to the partition, thus messages with the same key will always end up in the same order in the same partition.  
It is possible to have a topic with just one partition, however this is not advisable for topics that will have a high volume of messages as this limits how much you can parallize the processing  

#### 1. Create a Topic  
Go to the bin directory where you downloaded and extracted confluent package to run  
``./kafka-topics --zookeeper $(boot2docker ip):2181 --create --topic benders --partitions 3 --replication-factor 3`` 
*"Note $(docker-machine ip default) if using dockermachine"*

   This should create a topic with 3 partitions with each partition having 3 copies.   
#### 2. View Topic information  
To view the list of topics, run  
``./kafka-topics --zookeeper $(boot2docker ip):2181 --list``   
*"Note $(docker-machine ip default) if using dockermachine"*

   This will return something similar to the following (*ignore the _schema topic, it's auto-created by the schemaregistry container*)  
   > benders

To view more detailed information about the topic (Partitions, Partition leaders, Replicas, In Sync Replica Set) run  
``./kafka-topics --zookeeper $(boot2docker ip):2181 --describe``  
*"Note $(docker-machine ip default) if using dockermachine"*

   This will return something similar to the following
   > Topic:benders	PartitionCount:3	ReplicationFactor:3	Configs:  
   >	Topic: benders	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1  
   >    Topic: benders	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2  
   >    Topic: benders	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3  

   This essentially means the following (for Partition 0 on the second line)  
   - The leader for partition 0 is broker 2  
   - There are 3 copies for partition 0, with the copies residing on brokers 2, 3 and 1  
   - All three copies of partition 0 are up to date and are said to be in ISR (In Sync Replica Set)  
   Note The leader for a partition is the broker that all client reads and writes are carried out with. The other brokers only keep up with the leader for that partition and won't handle external client read/write requests  

## Sending messages
We are going to use Kafka's own java console producer to send the messages to kafka cluster.   
This producer takes a list of brokers and uses this list to get meta-data about all the topics in the cluster.   
This meta-data contains information about the all the topic partitions, but most importantly which broker is the leader for each partition at this moment in time.  
The producer by default will hash any provided message keys, calculate the modulus (using the number of partition) to determine which partiton to sent the message to, then use the meta-data to know which broker to send the message(s) to.  
If no message key is provided, it will send the message(s) to a randomly chosen partition and send continously for a certain period.   
*Please bear in mind, this is how the java producer used by the console tool behaves, not necessarily how other producers behave*  

#### 1. Send Messages (to non-specific partitions)
Run  
`` echo '{"name":"Ang", "skills":["fire-bending", "water-bending", "earth-bending", "air-bending"]}' | ./kafka-console-producer --broker-list $(boot2docker ip):9092 --topic benders``  
`` echo '{"name":"Korra", "skills":["fire-bending", "water-bending", "earth-bending", "air-bending"]}' | ./kafka-console-producer --broker-list $(boot2docker ip):9092 --topic benders``  

Since we didn't provide any keys, these messages will end up on any partition. Luckily we can run a command to consume messages from all partitions, so to see these messages run  
``./kafka-console-consumer --zookeeper $(boot2docker ip):2181 --topic benders --from-beginning``  
This will consume messages from all partitions from the beginnning (and will continue to consume), you should see something similar to this  
> {"name":"Ang", "skills":["fire-bending", "water-bending", "earth-bending", "air-bending"]}  
> {"name":"Korra", "skills":["fire-bending", "water-bending", "earth-bending", "air-bending"]}  


#### 2. Send Messages (to Specific partitions)
*Please note the extra properties passed to the console-producer command (--property)*  
Run  
``./kafka-console-producer --property parse.key=true --property key.separator="--" --broker-list $(boot2docker ip):9092 --topic benders``  
Now enter the following lines  
``1--{"name":"Zuko", "skills":["fire-bending"]}``  
``1--{"name":"Azula", "skills":["fire-bending"]}``  
``1--{"name":"Mako", "skills":["fire-bending"]}``  
``2--{"name":"Toph Beifong", "skills":["earth-bending"]}``  
``2--{"name":"Bolin", "skills":["earth-bending"]}``  
``3--{"name":"Katara", "skills":["water-bending"]}``  
``3--{"name":"Kya", "skills":["water-bending"]}``  

Hit Control-C to stop sending messages.  
What should have happened is that the entries with the same key (number) will have ended up in the same partitions, in the order that they were entered.  
Thus all the fire,water and earth benders should end up on their own partitions.   
*Don't be suprised if you see Ang and Korra on any of the partitions as they were sent without any keys and can appear any where, again bear in mind this is the default behaviour of the java kafka producer*   


## Consuming messages
As you've probably guessed by now, we can consume messages from all a Topic's partitions or from a specific partition.  

#### Consume messages from all partitions   
To consume all the messages that have been sent to all the partitions of the bender topic run   
``./kafka-console-consumer --property print.key=true --zookeeper $(boot2docker ip):2181 --topic benders --from-beginning``  
*This console consumer implementation will read the partitions in no specific order, but the order of the messages with the same key should be maintained (run the command multiple times to better understand)*

#### Consume messages from particular partitions   
*Please note the extra property passed to the console-consumer command (--property)*  
To consume all the messages that have been sent to partition 0 of the bender topic run   
``./kafka-run-class kafka.tools.SimpleConsumerShell --property print.key=true --broker-list "$(boot2docker ip):9092" --topic benders --partition 0 --offset -2``  

To consume all the messages that have been sent to partition 1 of the bender topic run   
``./kafka-run-class kafka.tools.SimpleConsumerShell --property print.key=true--broker-list "$(boot2docker ip):9092" --topic benders --partition 1 --offset -2``  

To consume all the messages that have been sent to partition 2 of the bender topic run   
``./kafka-run-class kafka.tools.SimpleConsumerShell --property print.key=true--broker-list "$(boot2docker ip):9092" --topic benders --partition 2 --offset -2``  


## Balancing Partitions.
Kafka aims to balance the partitions and partition leaders (default configuration) evenly across the brokers.
To see this in action, we are going to shut down a broker, look at how kafka rebalances and then bring the broker back up.   
1) Stop Broker 3 by running ``docker-compose stop kafka3``  
   After the broker has been stopped, view the partitions by running ``/kafka-topics --zookeeper $(boot2docker ip):2181 --describe``  
   You should see something similar to the following  
   > Topic:benders	PartitionCount:3	ReplicationFactor:3	Configs:  
   >	Topic: benders	Partition: 0	Leader: 1	Replicas: 3,1,2	Isr: 1,2  
   >	Topic: benders	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2  
   >	Topic: benders	Partition: 2	Leader: 2	Replicas: 2,3,1	Isr: 2,1  

   As can be seen from the above, Broker 1 is the leader for partitions 0 and 1, whilst Broker 2 is the leader for only partition2  
   It can also be seen that each partition doesn't have a full ISR (i.e all replicas are not in the ISR)  
   Also the cluster can still function in this state, but with the possibility that Broker 1 will be dealing with twice the load/traffic that Broker 2 will be dealing with.  
   Send another message by running ``echo '1--{"name":"Iroh", "skills":["fire-bending"]}' | ./kafka-console-producer --property parse.key=true --property key.separator="--"  --broker-list $(boot2docker ip):9092 --topic benders``  
   Now re-run the previous consuming commands, you should see Iroh when consuming all partitions or just the partition that the fire benders are on.  

2) Start Broker 3 by running ``docker-compose start kafka3``
   After the broker has started, view the partitions again by running ``/kafka-topics --zookeeper $(boot2docker ip):2181 --describe``  
   You'll need to give the kafka cluster some time (a few minutes) before it rebalances the partition leaders and you'll see something similar to the following  
   > Topic:benders	PartitionCount:3	ReplicationFactor:3	Configs:  
   >	Topic: benders	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 1,2,3  
   >	Topic: benders	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3  
   >	Topic: benders	Partition: 2	Leader: 2	Replicas: 2,3,1	Isr: 2,1,3  
   
   The above output shows that all the ISR for all the partitions are full, but more importantly that each broker is now only responsible for just one partition  


## Useful Information  
To find out the actual ips of all containers run   
``docker ps | cut -d' ' -f1 | tail -n +2 | xargs -i {} docker inspect --format '{{.config.image }} {{ .networksettings.ipaddress }}' {}``  
*In actuality these are the ip's that you should use in the following commands, but since we've exposed the ports to the docker host, we'll use the localhost for linux and $(boot2docker ip) for osx*
###  Kafka Commands
- **Create a topic**   
  ``./kafka-topics --zookeeper $(boot2docker ip):2181 --create --topic test --partitions 1 --replication-factor 1``  
 you can experiment with the partition numbers and replication-factor  
 a personal rule of thumb is to have the number of partitions and replicas be multiples of the number of nodes 

- **List all available topics**   
  ``./kafka-topics --zookeeper $(boot2docker ip):2181 --list``  

- **Detailed list of topics, partitions and partition leaders**   
  ``./kafka-topics --zookeeper $(boot2docker ip):2181 --describe``   

- **Send a message to a topic**   
  ``echo 'Fear is the mind killer' | ./kafka-console-producer --broker-list $(boot2docker ip):9092 --topic test``  

- **To send a bunch of messages (manually) via the console (press enter to send each message)**   
  ``./kafka-console-producer --broker-list $(boot2docker ip):9092 --topic test``  

- **To continuously consume the latest messages from a topic and all it's partitions**   
  ``./kafka-console-consumer --zookeeper $(boot2docker ip):2181 --topic test``   

- **To consume ALL the messages from a topic and all it's partitions from the beginning**    
  ``./kafka-console-consumer --zookeeper $(boot2docker ip):2181 --topic test --from-beginning``   

- **To consume A message from A particular topic and A particular partition from the begining**   
  ``./kafka-run-class kafka.tools.SimpleConsumerShell --broker-list "$(boot2docker ip):9092" --max-messages 1 --topic test --partition 0 --offset -2``  

- **To consume A message from A particular topic and A particular partition from the latest**   
  ``./kafka-run-class kafka.tools.SimpleConsumerShell --broker-list "$(boot2docker ip):9092" --max-messages 1 --topic test --partition 0 --offset -1``  

### Useful Kafka links
*Remove the `.sh` part of the commands*  
1) More Topic and partition tools [page](https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools)  
