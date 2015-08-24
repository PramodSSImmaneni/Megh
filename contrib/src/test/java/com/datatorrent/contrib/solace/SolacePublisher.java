package com.datatorrent.contrib.solace;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.solacesystems.jcsmp.*;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * Created by pramod on 8/21/15.
 */
public class SolacePublisher implements Runnable
{
  final JCSMPFactory factory = JCSMPFactory.onlyInstance();
  JCSMPSession session = null;
  Destination destination;
  XMLMessageProducer messageProducer;
  ExecutorService executor;
  Random random = new Random();
  boolean published;
  int publishCount = 1000;
  int recvCount;

  JCSMPProperties properties;
  String destinationName;
  EndpointProperties endpointProperties;

  public SolacePublisher(JCSMPProperties properties, String destinationName, EndpointProperties endpointProperties) {
    this.properties = properties;
    this.destinationName = destinationName;
    this.endpointProperties = endpointProperties;
    executor = Executors.newSingleThreadExecutor();
  }

  public void connect() throws JCSMPException
  {
    try {
      session = factory.createSession(properties);
      session.connect();
      if (endpointProperties == null) {
        destination = factory.createTopic(destinationName);
      } else {
        destination = factory.createQueue(destinationName);
        session.provision((Endpoint) destination, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
      }
      messageProducer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler()
      {
        @Override
        public void handleError(String s, JCSMPException e, long l)
        {
          DTThrowable.rethrow(e);
        }

        @Override
        public void responseReceived(String s)
        {
          //System.out.println("Response recieved " + s);
          if (++recvCount == publishCount) {
            published = true;
          }
        }
      });
    } catch (JCSMPException e) {
      disconnect();
      throw e;
    }
  }

  public void disconnect() {
    executor.shutdown();
    try {
      executor.awaitTermination(2000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      DTThrowable.rethrow(e);
    } finally {
      if (session != null) {
        session.closeSession();
      }
    }
  }

  public void publish() {
    recvCount = 0;
    published = false;
    executor.submit(this);
  }

  public void run() {
    for (int i = publishCount; i-- > 0;) {
      TextMessage message = factory.createMessage(TextMessage.class);
      message.setText("Hello World " + random.nextInt(1000));
      message.setDeliveryMode(DeliveryMode.PERSISTENT);
      try {
        messageProducer.send(message, destination);
      } catch (JCSMPException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

}
