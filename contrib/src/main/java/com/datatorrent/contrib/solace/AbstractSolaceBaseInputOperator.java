package com.datatorrent.contrib.solace;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.lib.io.IdempotentStorageManager;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.CheckpointListener;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Created by pramod on 8/24/15.
 */
public abstract class AbstractSolaceBaseInputOperator<T> extends BaseOperator implements InputOperator, Operator.ActivationListener<Context.OperatorContext>, CheckpointListener
{

  @NotNull
  protected JCSMPProperties properties = new JCSMPProperties();

  protected IdempotentStorageManager idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();

  protected transient JCSMPFactory factory;
  protected transient JCSMPSession session;

  protected transient Consumer consumer;

  protected transient int operatorId;
  protected long windowId;
  protected transient long lastCompletedWId;

  int spinMillis;

  @Override
  public void setup(Context.OperatorContext context)
  {
    operatorId = context.getId();
    spinMillis = context.getValue(com.datatorrent.api.Context.OperatorContext.SPIN_MILLIS);
    factory = JCSMPFactory.onlyInstance();
    try {
      session = factory.createSession(properties);
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
    idempotentStorageManager.setup(context);
    lastCompletedWId = idempotentStorageManager.getLargestRecoveryWindow();
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    try {
      session.connect();
      consumer = getConsumer();
      consumer.start();
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void deactivate()
  {
    try {
      consumer.stop();
      clearConsumer();
      consumer.close();
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void teardown()
  {
    session.closeSession();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.windowId = windowId;
  }

  @Override
  public void emitTuples()
  {
    try {
      BytesXMLMessage message = consumer.receive(spinMillis);
      if (message != null) {
        processMessage(message);
      }
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  protected T processMessage(BytesXMLMessage message)
  {
    T tuple = convert(message);
    emitTuple(tuple);
    return tuple;
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

  protected abstract Consumer getConsumer() throws JCSMPException;

  protected abstract void clearConsumer() throws JCSMPException;

  protected abstract T convert(BytesXMLMessage message);

  protected abstract void emitTuple(T tuple);

  public JCSMPProperties getProperties()
  {
    return properties;
  }

  public void setProperties(JCSMPProperties properties)
  {
    this.properties = properties;
  }

  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return idempotentStorageManager;
  }

  public void setIdempotentStorageManager(IdempotentStorageManager idempotentStorageManager)
  {
    this.idempotentStorageManager = idempotentStorageManager;
  }

}
