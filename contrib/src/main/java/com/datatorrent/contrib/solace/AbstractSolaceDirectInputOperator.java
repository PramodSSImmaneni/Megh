package com.datatorrent.contrib.solace;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

/**
 * Created by pramod on 8/24/15.
 */
public abstract class AbstractSolaceDirectInputOperator extends AbstractSolaceBaseInputOperator implements InputOperator, Operator.ActivationListener<com.datatorrent.api.Context.OperatorContext>
{
  @NotNull
  protected String topicName;

  protected long endpointProvisionFlags = JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS;

  private transient Topic topic;

  @Override
  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    super.setup(context);
    // Create a handle to topic locally
    topic = factory.createTopic(topicName);
  }

  @Override
  protected Consumer getConsumer() throws JCSMPException
  {
    session.addSubscription(topic);
    return session.getMessageConsumer((XMLMessageListener)null);
  }

  @Override
  protected void processMessage(BytesXMLMessage message)
  {
    processDirectMessage(message);
  }

  protected abstract void processDirectMessage(BytesXMLMessage message);

  public String getTopicName()
  {
    return topicName;
  }

  public void setTopicName(String topicName)
  {
    this.topicName = topicName;
  }

}
