package com.findwise.hydra.activemq;

import com.findwise.hydra.PipelineServer;
import com.findwise.hydra.local.AmqConstants;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import java.io.IOException;

public class AmqServer extends Thread implements PipelineServer, MessageListener {

	private final Logger logger = LoggerFactory.getLogger(AmqServer.class);

	private MessageConsumer consumer;

	private ConnectionFactory connectionFactory;

	private Session session;

	private BrokerService broker;

	public AmqServer(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@Override
	public void run() {
		broker = new BrokerService();
		// configure the broker
		broker.setBrokerName("hydra-core");
		broker.setUseJmx(false);
		try {
			broker.start();
			Connection connection = connectionFactory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			consumer = session.createConsumer(session.createQueue(AmqConstants.CORE_QUEUE));
			consumer.setMessageListener(this);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void onMessage(Message message) {
		logger.trace("Received message: {}", message);
	}

	@Override
	public boolean blockingStart() {
		start();
		return true;
	}

	@Override
	public boolean hasError() {
		return false;
	}

	@Override
	public Exception getError() {
		return null;
	}

	@Override
	public void shutdown() throws IOException {
		try {
			broker.stop();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
