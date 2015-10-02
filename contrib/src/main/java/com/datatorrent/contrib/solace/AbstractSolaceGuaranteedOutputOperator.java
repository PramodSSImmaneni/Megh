package com.datatorrent.contrib.solace;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

/**
 * Created by pramod on 10/2/15.
 */
public abstract class AbstractSolaceGuaranteedOutputOperator<T> extends AbstractSolaceBaseOutputOperator<T>
{
  @NotNull
  protected String queueName;

  @NotNull
  protected EndpointProperties endpointProperties = new EndpointProperties();

  @Override
  protected Destination getDestination() throws JCSMPException
  {
    Queue queue = factory.createQueue(queueName);
    session.provision(queue, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
    return queue;
  }

  @Override
  protected Producer getProducer() throws JCSMPException
  {
    return session.getMessageProducer(null);
  }

  public String getQueueName()
  {
    return queueName;
  }

  public void setQueueName(String queueName)
  {
    this.queueName = queueName;
  }

  public EndpointProperties getEndpointProperties()
  {
    return endpointProperties;
  }

  public void setEndpointProperties(EndpointProperties endpointProperties)
  {
    this.endpointProperties = endpointProperties;
  }
}
