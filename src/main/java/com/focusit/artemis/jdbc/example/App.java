package com.focusit.artemis.jdbc.example;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.postgresql.ds.PGPoolingDataSource;

import javax.jms.*;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by doki on 16.02.17.
 */
public class App {
    private static final int MESSAGE_COUNT = 100*1000;

    public static void main(String[] args) throws Exception {
        EmbeddedJMS jms = new EmbeddedJMS();
        Configuration cfg = new ConfigurationImpl();

        // Step 1. Setup a datasource
        PGPoolingDataSource source = new PGPoolingDataSource();
        source.setDataSourceName("A Data Source");
        source.setServerName("localhost");
        source.setDatabaseName("artemis");
        source.setUser("artemis");
        source.setPassword("artemis");
        source.setMaxConnections(10);

        // Step 2. Prepare jdbc configuration for artemis
        DatabaseStorageConfiguration jdbcStorage = new DatabaseStorageConfiguration();
        jdbcStorage.setDataSource(source);
        jdbcStorage.setSqlProvider(JDBCUtils.getSQLProviderFactory("postgres"));

        cfg.setStoreConfiguration(jdbcStorage);

        // Step 3. Set default settings in the artemis configuration
        cfg.setSecurityEnabled(false);
        cfg.addAcceptorConfiguration
                (new TransportConfiguration(NettyAcceptorFactory.class.getName())).addConnectorConfiguration("connector", new TransportConfiguration(NettyConnectorFactory.class.getName()));

        // Step 4. Define queue
        JMSConfiguration jmsConfig = new JMSConfigurationImpl();
        JMSQueueConfiguration queueConfig = new JMSQueueConfigurationImpl();
        queueConfig.setName("queue1");
        queueConfig.setDurable(true);
        queueConfig.setBindings("queue/queue1");

        jmsConfig.getQueueConfigurations().add(queueConfig);

        // Step 5. Define connection factory
        ConnectionFactoryConfiguration cfConfig = new ConnectionFactoryConfigurationImpl().setName("cf").setConnectorNames(Arrays.asList("connector")).setBindings("cf");
        jmsConfig.getConnectionFactoryConfigurations().add(cfConfig);

        // Step 6. Make artemis use prepared configuration
        jms.setConfiguration(cfg).setJmsConfiguration(jmsConfig);

        // Step 7. Start embedded artemis server
        jms.start();

        // Step 8. Open connection factory to embedded artemis and open the queue
        ConnectionFactory cf = (ConnectionFactory) jms.lookup("cf");
        Queue queue = (Queue) jms.lookup("queue/queue1");

        // Step 8. Send 50 000 simple messages using JMS API
        Connection connection = null;
        connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);

        // Sending
        for(int i=0;i<MESSAGE_COUNT;i++) {
            sendMessage(session, producer);
        }

        // Receiving
        for(int i=0;i<MESSAGE_COUNT;i++) {
            receiveMessage(queue, connection, session);
        }
        if (connection != null) {
            connection.close();
        }

        System.in.read();

        jms.stop();
    }

    // Sends a simple jms message to the queue
    private static void sendMessage(Session session, MessageProducer producer) throws JMSException {
        TextMessage message = session.createTextMessage("Hello sent at " + new Date());
        System.out.println("Sending message: " + message.getText());
        producer.send(message);
    }

    // Receives a simple jms message from the queue
    private static void receiveMessage(Queue queue, Connection connection, Session session) throws JMSException {
        MessageConsumer messageConsumer = session.createConsumer(queue);
        connection.start();
        TextMessage messageReceived = (TextMessage) messageConsumer.receive(1000);
        System.out.println("Received message:" + messageReceived.getText());
    }
}
