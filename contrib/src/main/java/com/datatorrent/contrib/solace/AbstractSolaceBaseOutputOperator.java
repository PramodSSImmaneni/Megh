package com.datatorrent.contrib.solace;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.ActivationListener;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Created by pramod on 10/2/15.
 */
public abstract class AbstractSolaceBaseOutputOperator<T> extends BaseOperator implements ActivationListener<OperatorContext>
{

  @NotNull
  protected JCSMPProperties properties = new JCSMPProperties();

  protected transient JCSMPFactory factory;
  protected transient JCSMPSession session;

  private transient Destination destination;

  private transient XMLMessageProducer xmlProducer;
  private transient DestinationProducer destProducer;

  @Override
  public void setup(Context.OperatorContext context)
  {
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
      Producer producer = getProducer();
      if (producer instanceof XMLMessageProducer) {
        xmlProducer = (XMLMessageProducer)producer;
      } else if (producer instanceof DestinationProducer) {
        destProducer = (DestinationProducer)producer;
      } else {
        throw new RuntimeException("Producer " + producer.getClass().getName() + " not supported");
      }
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void deactivate()
  {
    if (xmlProducer != null) {
      xmlProducer.close();
    } else if (destProducer != null) {
      destProducer.close();
    }
  }

  @Override
  public void teardown()
  {
    session.closeSession();
  }

  protected abstract Destination getDestination() throws JCSMPException;

  protected abstract Producer getProducer() throws JCSMPException;

  protected abstract XMLMessage getMessage(T tuple);

  protected void sendMessage(T tuple) {
    XMLMessage message = getMessage(tuple);
    try {
      if (xmlProducer != null) {
        xmlProducer.send(message, destination);
      } else {
        destProducer.send(message, destination);
      }
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }
}
