package com.findwise.hydra.local;

import com.findwise.hydra.Document;
import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.Query;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.InitFailedException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class AmqRemotePipeline implements RemotePipeline {

	private final Logger logger = LoggerFactory.getLogger(AmqRemotePipeline.class);

	private final ActiveMQConnectionFactory connectionFactory;

	private MessageConsumer consumer;

	private Destination stageDestination;

	private MessageProducer producer;

	private Session session;

	private final String stage;

	public AmqRemotePipeline(ActiveMQConnectionFactory connectionFactory, String stage) throws JMSException {
		this.connectionFactory = connectionFactory;
		this.stage = stage;
	}

	public void connect() throws Exception {
		Connection connection = connectionFactory.createConnection();
		connection.start();

		this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		stageDestination = session.createQueue(AmqConstants.STAGE_QUEUE_PREFIX + stage);
		this.consumer = session.createConsumer(stageDestination);

		Destination coreDestination = session.createQueue(AmqConstants.CORE_QUEUE);
		this.producer = session.createProducer(coreDestination);
	}

	@Override
	public LocalDocument getDocument(LocalQuery query) throws IOException {
		try {
			String correlationId = UUID.randomUUID().toString();
			TextMessage message = session.createTextMessage(query.toJson());
			message.setObjectProperty(AmqConstants.TYPE_PROPERTY, Query.class);
			message.setJMSReplyTo(stageDestination);
			message.setJMSCorrelationID(correlationId);
			producer.send(message);
			logger.trace("Sent query message: {}", message);
			Message reply = consumer.receive();
			logger.trace("Received reply: {}", reply);
			String body = ((TextMessage)reply).getText();
			return new LocalDocument(body);
		} catch (JMSException e) {
			throw new IOException(e);
		} catch (JsonException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean saveFull(LocalDocument d) throws IOException, JsonException {
		return false;
	}

	@Override
	public boolean save(LocalDocument d) throws IOException, JsonException {
		return false;
	}

	@Override
	public boolean markPending(LocalDocument d) throws IOException {
		return false;
	}

	@Override
	public boolean markFailed(LocalDocument d) throws IOException {
		return false;
	}

	@Override
	public boolean markFailed(LocalDocument d, Throwable t) throws IOException {
		return false;
	}

	@Override
	public boolean markProcessed(LocalDocument d) throws IOException {
		return false;
	}

	@Override
	public boolean markDiscarded(LocalDocument d) throws IOException {
		return false;
	}

	@Override
	public AbstractProcessStage getStageInstance() throws IOException, IllegalAccessException, InitFailedException, InstantiationException, JsonException, RequiredArgumentMissingException, ClassNotFoundException {
		return null;
	}

	@Override
	public String getStageName() {
		return null;
	}

	@Override
	public boolean isPerformanceLogging() {
		return false;
	}

	@Override
	public DocumentFile<Local> getFile(String fileName, DocumentID<Local> docid) {
		return null;
	}

	@Override
	public List<DocumentFile<Local>> getFiles(DocumentID<Local> docid) {
		return null;
	}

	@Override
	public List<String> getFileNames(DocumentID<?> docid) {
		return null;
	}

	@Override
	public boolean deleteFile(String fileName, DocumentID<Local> docid) {
		return false;
	}

	@Override
	public boolean saveFile(DocumentFile<Local> df) {
		return false;
	}
}
