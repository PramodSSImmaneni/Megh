package com.datatorrent.contrib.solace;

import com.solacesystems.jcsmp.*;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Context;

/**
 * Created by pramod on 8/20/15.
 */
public class AbstractSolaceInputOperatorTest
{
  @Test
  public void testDirectSolace() {
    JCSMPProperties properties = new JCSMPProperties();
    properties.setProperty(JCSMPProperties.HOST, "192.168.128.131:55555");
    properties.setProperty(JCSMPProperties.VPN_NAME, "default");
    properties.setProperty(JCSMPProperties.USERNAME, "pramod");

    final JCSMPFactory factory = JCSMPFactory.onlyInstance();
    JCSMPSession session = null;
    try {
      session = factory.createSession(properties);
      session.connect();
      System.out.println("Compression capable " + session.getCapability(CapabilityType.COMPRESSION));
      Topic topic = factory.createTopic("topic1");
      TextMessage message = factory.createMessage(TextMessage.class);
      message.setText("Hello World");
      session.addSubscription(topic);
      XMLMessageConsumer messageConsumer = session.getMessageConsumer(new XMLMessageListener()
      {
        @Override
        public void onReceive(BytesXMLMessage bytesXMLMessage)
        {
          System.out.println("Recieved message " + ((TextMessage)bytesXMLMessage).getText());
        }

        @Override
        public void onException(JCSMPException e)
        {
          System.out.println("Recieve exception " + e);
        }
      });
      XMLMessageProducer messageProducer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler()
      {
        @Override
        public void handleError(String s, JCSMPException e, long l)
        {
          System.out.println("Exception " + e);
        }

        @Override
        public void responseReceived(String s)
        {
          System.out.println("Response recieved " + s);
        }
      });
      messageConsumer.start();
      messageProducer.send(message, topic);
      Thread.sleep(30000);
      messageConsumer.stop();
    } catch (InvalidPropertiesException e) {
      e.printStackTrace();
    } catch (JCSMPException e) {
      e.printStackTrace();
      assert false;
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (session != null) {
        session.closeSession();
      }
    }
  }

  @Test
  public void testQueueSolace() {
    JCSMPProperties properties = new JCSMPProperties();
    properties.setProperty(JCSMPProperties.HOST, "192.168.128.131:55555");
    properties.setProperty(JCSMPProperties.VPN_NAME, "default");
    properties.setProperty(JCSMPProperties.USERNAME, "pramod");

    final JCSMPFactory factory = JCSMPFactory.onlyInstance();
    JCSMPSession session = null;
    try {
      session = factory.createSession(properties);
      session.connect();
      System.out.println("Compression capable " + session.getCapability(CapabilityType.COMPRESSION));
      EndpointProperties endpointProperties = new EndpointProperties();
      //endpointProperties.setPermission(EndpointProperties.PERMISSION_CONSUME);
      //endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
      Queue queue = factory.createQueue("MyQ");
      session.provision(queue, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
      //Topic topic = factory.createTopic("topic1");
      TextMessage message = factory.createMessage(TextMessage.class);
      message.setText("Hello World");
      message.setDeliveryMode(DeliveryMode.PERSISTENT);
      /*
      session.addSubscription(topic);
      XMLMessageConsumer messageConsumer = session.getMessageConsumer(new XMLMessageListener()
      {
        @Override
        public void onReceive(BytesXMLMessage bytes XMLMessage)
        {
          System.out.println("Recieved message " + ((TextMessage)bytesXMLMessage).getText());
        }

        @Override
        public void onException(JCSMPException e)
        {
          System.out.println("Recieve exception " + e);
        }
      });
      */
      XMLMessageProducer messageProducer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler()
      {
        @Override
        public void handleError(String s, JCSMPException e, long l)
        {
          System.out.println("Exception " + e);
        }

        @Override
        public void responseReceived(String s)
        {
          System.out.println("Response recieved " + s);
        }
      });
      /*
      messageConsumer.start();
      messageProducer.send(message, topic);
      Thread.sleep(30000);
      messageConsumer.stop();
      */
      messageProducer.send(message, queue);
      Thread.sleep(100);
      ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
      flowProperties.setEndpoint(queue);
      flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
      FlowReceiver receiver = session.createFlow(null, flowProperties, endpointProperties);
      receiver.start();
      BytesXMLMessage recvMessage = receiver.receive(30000);
      if (recvMessage != null) {
        System.out.println("Received message :" + recvMessage.dump());
        recvMessage.ackMessage();
      }
      receiver.close();
    } catch (InvalidPropertiesException e) {
      e.printStackTrace();
    } catch (JCSMPException e) {
      e.printStackTrace();
      assert false;
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (session != null) {
        session.closeSession();
      }
    }
  }

  @Test
  public void testOperator() {
    JCSMPProperties properties = new JCSMPProperties();
    properties.setProperty(JCSMPProperties.HOST, "192.168.128.131:55555");
    properties.setProperty(JCSMPProperties.VPN_NAME, "default");
    properties.setProperty(JCSMPProperties.USERNAME, "pramod");
    SolacePublisher publisher = new SolacePublisher(properties);
    SolaceTextInputOperator inputOperator = new SolaceTextInputOperator();
    try {
      publisher.connect();
      publisher.publish();

      inputOperator.setProperties(properties);
      inputOperator.setQueue("MyQ");

      CollectorTestSink sink = new CollectorTestSink();
      inputOperator.output.setSink(sink);

      Context.OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(0);
      inputOperator.setup(context);
      inputOperator.activate(context);

      inputOperator.beginWindow(0);
      for (int i = 0; i < 1000; ++i) {
        inputOperator.emitTuples();
      }
      inputOperator.endWindow();

      inputOperator.deactivate();
      inputOperator.teardown();

      System.out.println("TOTAL TUPLES " + sink.collectedTuples.size());
      for (String s : ((CollectorTestSink<String>)sink).collectedTuples) {
        System.out.println("Received " + s);
      }
    } catch (JCSMPException e) {
      Assert.fail(e.getMessage());
    } finally {
      publisher.disconnect();
    }
  }

}
