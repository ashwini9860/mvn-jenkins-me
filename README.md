# AEF Engine

## This module contains

* API for Kafka interaction

## Building
mvn clean install

##Generate documentation
mvn clean install -P javadoc

## Using the API
Any module that uses this library needs to provide `spring-kafka`, perform 
the component scan in `com.alice.aef.client` and configure the kafka connection in 
its `application.properties` file.

Also, the client can be configured with 
the timeout of synchronous calls and the polling frequency through 
`alice.aef.client.timeout` and `alice.aef.client.frequency`, both in 
milliseconds. By default the timeout is 60000 ms and the frequency is 1000 ms.

## Using AEFClient  
Event fabric client designed to send events to a topic.  
Topic can be specified during construction (ctor param) or via method param.  

Create client  
`new AEFClient();`  
`new AEFClient(REQUEST_TOPIC_1);`  

Send messages  
` client.send(data);`  
Specify destination topic  
` client.send(REQUEST_TOPIC_2, data);`  

## Using AEFConsumerClient  
Event fabric client designed to subscribe to a topic and receive events from it.  
Consumer group id, topics and message listener should be specified during construction.  
`Important note is that consumer group id is mandatory - this way all beans with the same group id
   on all instances will belong to the same kafka consumer group.`
  
```
String requestTopic = serviceRegistry.getRequestTopic("alice-absolem-service", "write");
// subscribe to topics and provide a message handler (writerService)`  
AEFConsumerClient consumerClient = new AEFConsumerClient("groupId", Lists.list(requestTopic), writerService());
```
groupId - consumer group id can be configured via application.properties file
(preferably use some unique, service specific name)  

## Using AEFRequestReplyClient  
Event fabric client designed to send events to kafka and subscribe/await a reply.  
Consumer group id and reply topics should be specified during construction.  
`Important note is that consumer group id is mandatory - this way all beans with the same group id
  on all instances will belong to the same kafka consumer group.`  

Creating request reply client  
```
String parseResponseTopic = serviceRegistry.getResponseTopic("alice-absolem-service", "parse");
String parseErrorTopic = serviceRegistry.getErrorTopic("alice-absolem-service", "parse");
List<String> replyTopics = new ArrayList<>();
replyTopics.add(parseResponseTopic);
replyTopics.add(parseErrorTopic);
AEFRequestReplyClient requestReplyClient = new AEFRequestReplyClient("groupId", replyTopics);
```
groupId - consumer group id can be configured via application.properties file
(preferably use some unique, service specific name) 

Sending request and waiting for a reply  
```
ConsumerRecord<String, Map<String, Object>> responseMessage = requestReplyClient.sendAndReceive(requestTopic, params);
```