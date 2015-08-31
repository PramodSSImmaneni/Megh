package com.datatorrent.contrib.solace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.lib.io.IdempotentStorageManager;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * Created by pramod on 8/24/15.
 */
public abstract class AbstractSolaceDirectInputOperator<T> extends AbstractSolaceBaseInputOperator implements InputOperator, Operator.ActivationListener<com.datatorrent.api.Context.OperatorContext>, Operator.CheckpointListener
{
  @NotNull
  protected String topicName;

  private transient Topic topic;

  private IdempotentStorageManager idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();

  private transient int operatorId;
  private transient long windowId;
  private transient long lastCompletedWId;
  private transient List<T> tuples = new ArrayList<T>();

  @Override
  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    super.setup(context);
    operatorId = context.getId();
    idempotentStorageManager.setup(context);
    lastCompletedWId = idempotentStorageManager.getLargestRecoveryWindow();
    // Create a handle to topic locally
    topic = factory.createTopic(topicName);
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    if (windowId <= lastCompletedWId) {
      try {
        tuples = (List<T>)idempotentStorageManager.load(operatorId, windowId);
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void endWindow()
  {
    try {
      if (windowId <= lastCompletedWId) {
        processRecoveredMessages();
      } else {
        idempotentStorageManager.save(tuples, operatorId, windowId);
        tuples.clear();
      }
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void emitTuples()
  {
    if (windowId <= lastCompletedWId) {
      processRecoveredMessages();
    } else {
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
  protected void processMessage(BytesXMLMessage message)
  {
    T tuple = convert(message);
    tuples.add(tuple);
    emitTuple(tuple);
  }

  private void processRecoveredMessages() {
    for (T tuple : tuples) {
      //processDirectMessage(message);
      emitTuple(tuple);
    }
    tuples.clear();
  }

  @Override
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long window)
  {
    try {
      idempotentStorageManager.deleteUpTo(operatorId, window);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  protected abstract T convert(BytesXMLMessage message);

  protected abstract void emitTuple(T tuple);

  public String getTopicName()
  {
    return topicName;
  }

  public void setTopicName(String topicName)
  {
    this.topicName = topicName;
  }

}
