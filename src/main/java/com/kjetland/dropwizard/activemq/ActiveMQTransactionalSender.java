package com.kjetland.dropwizard.activemq;

import javax.jms.Message;
import javax.jms.Session;

public interface ActiveMQTransactionalSender {

	JmsSession sendJson(String json);

	JmsSession send(Object object);

	JmsSession send(JMSFunction<Session, Message> messageCreator);

	public interface JmsSession extends AutoCloseable {
		void close();
		void rollback();
		void commit();
	}

}
