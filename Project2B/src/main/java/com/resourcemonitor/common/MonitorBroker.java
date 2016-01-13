package com.resourcemonitor.common;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Encapsulate the broker interactions.
 */
public class MonitorBroker {
    // session
    private Session session;
    // producer
    private MessageProducer producer;
    // consumer
    private MessageConsumer consumer;
    private String url;
    private String topicName;

    /**
     * Create the broker
     */
    public MonitorBroker(boolean sender) {
        url = Configuration.getInstance().getBrokerUrl();
        topicName = Configuration.getInstance().getTopicName();

        try {
            init(sender);
        } catch (MonitorException e) {
            e.printStackTrace();
            System.out.println("Failed to initialize the system..");
            System.exit(1);
        }
    }

    /**
     * Initialize the connections. Create the session, producer and consumer
     * @param sender weather this is a producer or a consumer, student can choose
     *               not to create the producer, when sender = false and student can
     *               choose not to create the consumer when sender = true
     * @throws MonitorException
     */
    private void init(boolean sender) throws MonitorException {

        /** implement your code
        It’s similar to the ActiveMQ example in lab
        1. create topic/queue connection session
        2. If it’s sender, create a producer, otherwise, create a consumer
        */

    	try {
    		//create a connect factory CF and connect to it
    		ActiveMQConnectionFactory CF = new ActiveMQConnectionFactory(url);
    		Connection connection = CF.createConnection();
    		//start the connection
    		connection.start();
	        //create a session using this connection
	        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	        //destination <--topic
	        Destination destination = session.createTopic(topicName);
	        if (sender) {
	        	//MessageProducer for sender
		        producer = session.createProducer(destination);
		        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
	        } else {
	        	//MessageConsumer otherwise
                consumer = session.createConsumer(destination);
	        }
		} catch (JMSException e) {
			e.printStackTrace();
		}
    	
    }

    /**
     * Create a JMS message using the Monitor Data and send using the producer
     * @param data MonitorData instance
     * @throws JMSException 
     */
    public void send(MonitorData data) throws JMSException {
        /** implement your code
           Producer send message to ActiveMQ broker
           1. Construct/set message body by using javax.jms.MapMessage
           2. Send the message
        */
    	
    	//create message: mes and send it
    	Message mes = session.createMessage();
    	mes.setDoubleProperty("cpu", data.getCpu());
    	mes.setDoubleProperty("memory", data.getMemory());
    	mes.setIntProperty("workerId", data.getWorkerId());
    	mes.setLongProperty("time", data.getTime());
    	mes.setBooleanProperty("process", data.isProcess());
    	producer.send(mes);
    	
    }

    /**
     * Receive a JMS message and convert it to MonitorData. After the monitor data is created call
     * the handler.onMessage(monitorData).
     * @param handler the handler to call with the monitor message
     * @throws JMSException 
     */
    public void startReceive(final ReceiveHandler handler) throws JMSException {
        /** implement your code
           Consumer receive messages from ActiveMQ broker
           1. If receive any message, deserialize it and fill them into MonitorData object
           2. Using handler.onMessage
        */
    	
    	//use:
    	//void setMessageListener(MessageListener listener) throws JMSException;
    	consumer.setMessageListener(
    			//new MessageListener(){} as listener
    			new MessageListener() {
    				//override: void onMessage(Message message);
    				public void onMessage(Message message){
    					MonitorData monitorData = new MonitorData();	
    					try {
    						monitorData.setCpu(message.getDoubleProperty("cpu"));
    						monitorData.setMemory(message.getDoubleProperty("memory"));
    						monitorData.setWorkerId(message.getIntProperty("workerId"));
    						monitorData.setTime(message.getLongProperty("time"));
    						monitorData.setProcess(message.getBooleanProperty("process"));
    					} catch (JMSException e) {
    						e.printStackTrace();
    					}
    					handler.onMessage(monitorData);
    				}
    			});
    	
    }

}
