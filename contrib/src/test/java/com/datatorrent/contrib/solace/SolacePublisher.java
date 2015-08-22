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
  Queue queue;
  XMLMessageProducer messageProducer;
  ExecutorService executor;
  Random random = new Random();

  JCSMPProperties properties;

  public SolacePublisher(JCSMPProperties properties) {
    this.properties = properties;
    executor = Executors.newSingleThreadExecutor();
  }

  public void connect() throws JCSMPException
  {
    try {
      session = factory.createSession(properties);
      session.connect();
      EndpointProperties endpointProperties = new EndpointProperties();
      //endpointProperties.setPermission(EndpointProperties.PERMISSION_CONSUME);
      //endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
      queue = factory.createQueue("MyQ");
      session.provision(queue, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
      messageProducer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler()
      {
        @Override
        public void handleError(String s, JCSMPException e, long l)
        {
          System.out.println("Exception " + e);
        }

        @Override
        public void responseReceived(String s)
        {
          //System.out.println("Response recieved " + s);
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
    executor.submit(this);
  }

  public void run() {
    for (int i = 1000; i-- > 0;) {
      TextMessage message = factory.createMessage(TextMessage.class);
      message.setText("Hello World " + random.nextInt(1000));
      message.setDeliveryMode(DeliveryMode.PERSISTENT);
      try {
        messageProducer.send(message, queue);
      } catch (JCSMPException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

}
