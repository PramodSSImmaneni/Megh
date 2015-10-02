package com.datatorrent.contrib.solace;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.Producer;

/**
 * Created by pramod on 10/2/15.
 */
public abstract class AbstractSolaceDirectOutputOperator<T> extends AbstractSolaceBaseOutputOperator<T>
{
  @NotNull
  protected String topicName;

  @Override
  protected Destination getDestination()
  {
    return factory.createTopic(topicName);
  }

  @Override
  protected Producer getProducer() throws JCSMPException
  {
    return session.getMessageProducer(null);
  }

  public String getTopicName()
  {
    return topicName;
  }

  public void setTopicName(String topicName)
  {
    this.topicName = topicName;
  }
}
