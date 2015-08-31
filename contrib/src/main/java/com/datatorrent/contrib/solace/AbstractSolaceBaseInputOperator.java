package com.datatorrent.contrib.solace;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Created by pramod on 8/24/15.
 */
public abstract class AbstractSolaceBaseInputOperator extends BaseOperator implements InputOperator, Operator.ActivationListener<Context.OperatorContext>
{

  @NotNull
  protected JCSMPProperties properties = new JCSMPProperties();

  protected transient JCSMPFactory factory;
  protected transient JCSMPSession session;

  protected transient Consumer consumer;

  int spinMillis;

  @Override
  public void setup(Context.OperatorContext context)
  {
    spinMillis = context.getValue(com.datatorrent.api.Context.OperatorContext.SPIN_MILLIS);
    factory = JCSMPFactory.onlyInstance();
    try {
      session = factory.createSession(properties);
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
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

  protected abstract Consumer getConsumer() throws JCSMPException;

  protected abstract void clearConsumer() throws JCSMPException;

  protected abstract void processMessage(BytesXMLMessage message);

  public JCSMPProperties getProperties()
  {
    return properties;
  }

  public void setProperties(JCSMPProperties properties)
  {
    this.properties = properties;
  }

}
