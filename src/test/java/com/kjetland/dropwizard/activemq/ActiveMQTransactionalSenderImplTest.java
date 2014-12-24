package com.kjetland.dropwizard.activemq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kjetland.dropwizard.activemq.ActiveMQTransactionalSender.JmsSession;

@RunWith(MockitoJUnitRunner.class)
public class ActiveMQTransactionalSenderImplTest {

	@Mock
	private ConnectionFactory connectionFactory;
	@Mock
	private Connection connection;
	@Mock
	private Session session;
	@Mock
	private Queue queue;
	@Mock
	private MessageProducer messageProducer;
	@Mock
	private ObjectMapper objectMapper;
	@Mock
	private TextMessage textMessage;

	@Before
	public void setUp() throws Exception {
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createSession(anyBoolean(), anyInt())).thenReturn(
				session);
	}

	@Test
	public void testSessionCreationFailed() throws JMSException {
		final String queueName = "myQueue";
		final JMSException thrownException = new JMSException("Test");
		when(connection.createSession(anyBoolean(), anyInt())).thenThrow(
				thrownException);
		final ActiveMQTransactionalSender sender = new ActiveMQTransactionalSenderImpl(
				connectionFactory, objectMapper, queueName,
				Optional.<Integer> empty(), false);

		try (JmsSession session = sender.send(null)) {
			fail("Expected JMSException was not thrown");
		} catch (RuntimeException re) {
			assertEquals(thrownException, re.getCause());
			verify(connection).close();
			verify(connection, Mockito.times(1)).createSession(true,
					Session.AUTO_ACKNOWLEDGE);
			verify(session, Mockito.times(0)).close();
		}
	}

	@Test
	public void testProducerCreationFailed() throws JMSException {
		final String queueName = "myQueue";
		final JMSException thrownException = new JMSException("Test");
		when(session.createProducer(Mockito.any())).thenThrow(thrownException);
		final ActiveMQTransactionalSender sender = new ActiveMQTransactionalSenderImpl(
				connectionFactory, objectMapper, queueName,
				Optional.<Integer> empty(), false);

		try (JmsSession session = sender.send(null)) {
			fail("Expected JMSException was not thrown");
		} catch (RuntimeException re) {
			assertEquals(thrownException, re.getCause());
			verify(connection).close();
			verify(session).close();
		}
	}

	@Test
	public void testSessionCloseFailed() throws Exception {
		final String queueName = "myQueue";
		final String myJson = "{'a': 2, 'b': 'Some text'}";
		final String myCorrelationId = UUID.randomUUID().toString();
		final ActiveMQTransactionalSender sender = new ActiveMQTransactionalSenderImpl(
				connectionFactory, objectMapper, queueName,
				Optional.<Integer> empty(), false);
		final JMSException thrownException = new JMSException("Test");

		when(session.createQueue(queueName)).thenReturn(queue);
		when(session.createProducer(queue)).thenReturn(messageProducer);
		when(session.createTextMessage()).thenReturn(textMessage);
		Mockito.doThrow(thrownException).when(session).close();

		// Send a message
		try (JmsSession jmsSession = sender.send((Session session) -> {
			TextMessage message = session.createTextMessage();
			message.setText(myJson);
			message.setJMSCorrelationID(myCorrelationId);
			message.setJMSReplyTo(queue);
			return message;
		})) {
		} catch (RuntimeException re) {
			assertEquals(thrownException, re.getCause());
			verify(session).close();
			verify(connection).close();
			return;
		}
		fail("Expected JMSException was not thrown");
	}
	
	@Test
	public void testConnectionCloseFailed() throws Exception {
		final String queueName = "myQueue";
		final String myJson = "{'a': 2, 'b': 'Some text'}";
		final String myCorrelationId = UUID.randomUUID().toString();
		final ActiveMQTransactionalSender sender = new ActiveMQTransactionalSenderImpl(
				connectionFactory, objectMapper, queueName,
				Optional.<Integer> empty(), false);
		final JMSException thrownException = new JMSException("Test");

		when(session.createQueue(queueName)).thenReturn(queue);
		when(session.createProducer(queue)).thenReturn(messageProducer);
		when(session.createTextMessage()).thenReturn(textMessage);
		Mockito.doThrow(thrownException).when(connection).close();

		// Send a message
		try (JmsSession jmsSession = sender.send((Session session) -> {
			TextMessage message = session.createTextMessage();
			message.setText(myJson);
			message.setJMSCorrelationID(myCorrelationId);
			message.setJMSReplyTo(queue);
			return message;
		})) {
		} catch (RuntimeException re) {
			assertEquals(thrownException, re.getCause());
			verify(session).close();
			verify(connection).close();
			return;
		}
		fail("Expected JMSException was not thrown");
	}

	@Test
	public void testSendSimpleQueueWithCreatorFunction() throws Exception {
		final String queueName = "myQueue";
		final String myJson = "{'a': 2, 'b': 'Some text'}";
		final String myCorrelationId = UUID.randomUUID().toString();
		final ActiveMQTransactionalSender sender = new ActiveMQTransactionalSenderImpl(
				connectionFactory, objectMapper, queueName,
				Optional.<Integer> empty(), false);

		when(session.createQueue(queueName)).thenReturn(queue);
		when(session.createProducer(queue)).thenReturn(messageProducer);
		when(session.createTextMessage()).thenReturn(textMessage);

		// Send a message
		try (JmsSession jmsSession = sender.send((Session session) -> {
			TextMessage message = session.createTextMessage();
			message.setText(myJson);
			message.setJMSCorrelationID(myCorrelationId);
			message.setJMSReplyTo(queue);
			return message;
		})) {
			/*
			 * sender.send((Session session) -> { try { TextMessage message =
			 * session.createTextMessage(); message.setText(myJson);
			 * message.setJMSCorrelationID(myCorrelationId);
			 * message.setJMSReplyTo(queue); return message; } catch
			 * (JMSException e) { throw new RuntimeException(e); } });
			 */

			// Verify that the message was constructed as intended
			verify(textMessage).setText(myJson);
			verify(textMessage).setJMSCorrelationID(myCorrelationId);
			verify(textMessage).setJMSReplyTo(queue);
			// Verify that the message was sent by the producer
			verify(messageProducer).send(textMessage);
			verify(messageProducer)
					.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			// Verify that everything was cleaned up afterwards
			verify(messageProducer).close();
			verify(session, Mockito.times(0)).close();
			verify(connection, Mockito.times(0)).close();
		}
		verify(session).close();
		verify(connection).close();

		verifyNoMoreInteractions(textMessage, messageProducer);
	}

	@Test
	public void testSendSimpleQueueWithCreatorFunctionWhenExceptionIsThrown()
			throws Exception {
		final String queueName = "myQueue";
		final String myJson = "{'a': 2, 'b': 'Some text'}";
		final String myCorrelationId = UUID.randomUUID().toString();
		final ActiveMQTransactionalSender sender = new ActiveMQTransactionalSenderImpl(
				connectionFactory, objectMapper, queueName,
				Optional.<Integer> empty(), false);
		final JMSException thrownException = new JMSException("Test");

		when(session.createQueue(queueName)).thenReturn(queue);
		when(session.createProducer(queue)).thenReturn(messageProducer);
		doThrow(thrownException).when(session).createTextMessage();

		// Send a message and verify that a wrapped RuntimeException is thrown
		try (JmsSession jmsSession = sender.send((Session session) -> {
			TextMessage message = session.createTextMessage();
			message.setText(myJson);
			message.setJMSCorrelationID(myCorrelationId);
			return message;
		})) {
			/*
			 * sender.send((Session session) -> { try { TextMessage message =
			 * session.createTextMessage(); message.setText(myJson);
			 * message.setJMSCorrelationID(myCorrelationId);
			 * message.setJMSReplyTo(queue); return message; } catch
			 * (JMSException e) { throw new RuntimeException(e); } });
			 */
			// We should not arrive here
			fail("Expected JMSException was not thrown");
		} catch (RuntimeException re) {
			assertEquals(thrownException, re.getCause());
		}
		// Verify that the message was not sent by the producer
		verify(messageProducer, never()).send(any(Message.class));
		// Verify that everything was cleaned up afterwards
		verify(messageProducer).close();
		verify(session).close();
		verify(connection).close();
	}

	@Test
	public void testSendJson() throws Exception {

	}

	@Test
	public void testSend1() throws Exception {

	}
}