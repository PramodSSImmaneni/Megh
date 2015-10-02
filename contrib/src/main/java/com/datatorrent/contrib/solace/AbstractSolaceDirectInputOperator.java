package com.datatorrent.contrib.solace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * Created by pramod on 8/24/15.
 */
public abstract class AbstractSolaceDirectInputOperator<T> extends AbstractSolaceBaseInputOperator<T> implements InputOperator, Operator.ActivationListener<com.datatorrent.api.Context.OperatorContext>
{
  @NotNull
  protected String topicName;

  private transient Topic topic;

  private transient List<T> tuples = new ArrayList<T>();

  @Override
  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    super.setup(context);
    // Create a handle to topic locally
    topic = factory.createTopic(topicName);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    if (windowId <= lastCompletedWId) {
      try {
        tuples = (List<T>)idempotentStorageManager.load(operatorId, windowId);
        handleRecovery();
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    try {
      if (windowId > lastCompletedWId) {
        idempotentStorageManager.save(tuples, operatorId, windowId);
      }
      tuples.clear();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void emitTuples()
  {
    if (windowId > lastCompletedWId) {
      super.emitTuples();
    }
  }

  @Override
  protected Consumer getConsumer() throws JCSMPException
  {
    session.addSubscription(topic);
    return session.getMessageConsumer((XMLMessageListener)null);
  }

  @Override
  protected void clearConsumer() throws JCSMPException
  {
    session.removeSubscription(topic);
  }

  @Override
  protected T processMessage(BytesXMLMessage message)
  {
    T tuple = super.processMessage(message);
    tuples.add(tuple);
    return tuple;
  }

  private void handleRecovery() {
    for (T tuple : tuples) {
      //processDirectMessage(message);
      emitTuple(tuple);
    }
    tuples.clear();
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
