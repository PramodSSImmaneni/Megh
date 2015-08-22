package com.datatorrent.contrib.solace;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 *
 */
public abstract class AbstractSolaceInputOperator extends BaseOperator implements InputOperator, Operator.ActivationListener<Context.OperatorContext>
{

  @NotNull
  private JCSMPProperties properties = new JCSMPProperties();
  @NotNull
  private EndpointProperties endpointProperties = new EndpointProperties();

  @NotNull
  private String queue;

  private long endpointProvisionFlags = JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS;

  private transient JCSMPFactory factory;
  private transient JCSMPSession session;

  private transient Endpoint endpoint;
  private transient FlowReceiver flowReceiver;

  @Override
  public void setup(Context.OperatorContext context)
  {
    /*
    JCSMPProperties properties = new JCSMPProperties();
    properties.setProperty(JCSMPProperties.HOST, "192.168.128.131:55555");
    properties.setProperty(JCSMPProperties.VPN_NAME, "default");
    properties.setProperty(JCSMPProperties.USERNAME, "pramod");
    */
    factory = JCSMPFactory.onlyInstance();
    try {
      session = factory.createSession(properties);
      // Create a handle to queue locally
      endpoint = factory.createQueue(this.queue);
      ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
      consumerFlowProperties.setEndpoint(endpoint);
      consumerFlowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
      flowReceiver = session.createFlow(null, consumerFlowProperties, endpointProperties);
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }


  @Override
  public void activate(Context.OperatorContext context)
  {
    try {
      session.connect();
      session.provision(endpoint, endpointProperties, endpointProvisionFlags);
      flowReceiver.start();
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void deactivate()
  {
    flowReceiver.close();
  }

  @Override
  public void teardown()
  {
    session.closeSession();
  }

  @Override
  public void emitTuples()
  {
    try {
      BytesXMLMessage message = flowReceiver.receive(10);
      if (message != null) {
        processMessage(message);
        message.ackMessage();
      }
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  protected abstract void processMessage(BytesXMLMessage message);

  public String getQueue()
  {
    return queue;
  }

  public void setQueue(String queue)
  {
    this.queue = queue;
  }

  public JCSMPProperties getProperties()
  {
    return properties;
  }

  public void setProperties(JCSMPProperties properties)
  {
    this.properties = properties;
  }

  public EndpointProperties getEndpointProperties()
  {
    return endpointProperties;
  }

  public void setEndpointProperties(EndpointProperties endpointProperties)
  {
    this.endpointProperties = endpointProperties;
  }

  public long getEndpointProvisionFlags()
  {
    return endpointProvisionFlags;
  }

  public void setEndpointProvisionFlags(long endpointProvisionFlags)
  {
    this.endpointProvisionFlags = endpointProvisionFlags;
  }
}
