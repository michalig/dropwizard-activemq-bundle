package com.kjetland.dropwizard.activemq;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ActiveMQTransactionalSenderImpl implements
		ActiveMQTransactionalSender {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final ConnectionFactory connectionFactory;
	private final ObjectMapper objectMapper;
	private final String destination;
	private final Optional<Integer> timeToLiveInSeconds;
	private final boolean persistent;
	protected final DestinationCreator destinationCreator = new DestinationCreatorImpl();

	public ActiveMQTransactionalSenderImpl(ConnectionFactory connectionFactory,
			ObjectMapper objectMapper, String destination,
			Optional<Integer> timeToLiveInSeconds, boolean persistent) {
		this.connectionFactory = connectionFactory;
		this.objectMapper = objectMapper;
		this.destination = destination;
		this.timeToLiveInSeconds = timeToLiveInSeconds;
		this.persistent = persistent;
	}

	@Override
	public JmsSession send(Object object) {
		try {
			final String json = objectMapper.writeValueAsString(object);
			return internalSend(json);
		} catch (Exception e) {
			throw new RuntimeException("Error sending to jms", e);
		}
	}

	@Override
	public JmsSession sendJson(String json) {
		try {
			return internalSend(json);
		} catch (Exception e) {
			throw new RuntimeException("Error sending to jms", e);
		}
	}

	private JmsSession internalSend(String json) throws JMSException {
		if (log.isDebugEnabled()) {
			log.debug("Sending to {}: {}", destination, json);
		}
		return internalSend(session -> {
			final TextMessage textMessage = session.createTextMessage(json);
			textMessage.setText(json);
			String correlationId = ActiveMQBundle.correlationID.get();
			if (textMessage.getJMSCorrelationID() == null
					&& correlationId != null) {
				textMessage.setJMSCorrelationID(correlationId);
			}
			return textMessage;
		});
	}

	private JmsSession internalSend(JMSFunction<Session, Message> messageCreator)
			throws JMSException {
		// Since we're using the pooled connectionFactory,
		// we can create connection, session and producer on the fly here.
		// as long as we do the cleanup / return to pool
		final Connection connection = connectionFactory.createConnection();
		try {
			final Session session = connection.createSession(true,
					Session.AUTO_ACKNOWLEDGE);
			try {
				final Destination d = destinationCreator.create(session,
						destination);
				final MessageProducer messageProducer = session
						.createProducer(d);
				try {
					messageProducer
							.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT
									: DeliveryMode.NON_PERSISTENT);
					if (timeToLiveInSeconds.isPresent()) {
						messageProducer.setTimeToLive(TimeUnit.SECONDS
								.toMillis(timeToLiveInSeconds.get()));
					}
					final Message message = messageCreator.apply(session);
					messageProducer.send(message);
				} finally {
					ActiveMQUtils.silent(() -> messageProducer.close());
				}
				return new JmsSessionImpl(session, connection);
			} catch (JMSException ex) {
				session.close();
				throw ex;
			}
		} catch (JMSException ex) {
			connection.close();
			throw ex;
		}
	}

	@Override
	public JmsSession send(JMSFunction<Session, Message> messageCreator) {
		// Since we're using the pooled connectionFactory,
		// we can create connection, session and producer on the fly here.
		// as long as we do the cleanup / return to pool
		try {
			return internalSend(messageCreator);
		} catch (JMSException jmsException) {
			throw new RuntimeException("Error sending to jms", jmsException);
		}
	}

	private class JmsSessionImpl implements JmsSession {

		private final Session session;
		private final Connection connection;

		public JmsSessionImpl(Session session, Connection connection) {
			this.session = session;
			this.connection = connection;
		}

		@Override
		public void rollback() {
			try {
				session.rollback();
			} catch (JMSException jmsException) {
				throw new RuntimeException("Error closing session",
						jmsException);
			}
		}

		@Override
		public void commit() {
			try {
				session.commit();
			} catch (JMSException jmsException) {
				throw new RuntimeException("Error closing session",
						jmsException);
			}
		}

		@Override
		public void close() {
			try {
				try {
					session.close();
				} catch (JMSException jmsException) {
					throw new RuntimeException("Error closing session",
							jmsException);
				} finally {
					connection.close();
				}
			} catch (JMSException jmsException) {
				throw new RuntimeException("Error closing session",
						jmsException);
			}
		}
	}

}
