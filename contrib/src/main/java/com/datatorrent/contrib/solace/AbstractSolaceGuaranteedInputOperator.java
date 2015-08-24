package com.datatorrent.contrib.solace;

import javax.validation.constraints.NotNull;

import com.solacesystems.jcsmp.*;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

/**
 *
 */
public abstract class AbstractSolaceGuaranteedInputOperator extends AbstractSolaceBaseInputOperator implements InputOperator, Operator.ActivationListener<Context.OperatorContext>
{

  @NotNull
  protected String endpointName;
  @NotNull
  protected EndpointType endpointType = EndpointType.QUEUE;

  @NotNull
  protected EndpointProperties endpointProperties = new EndpointProperties();

  protected long endpointProvisionFlags = JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS;

  private transient Endpoint endpoint;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    // Create a handle to endpoint locally
    if (endpointType == EndpointType.QUEUE) {
      endpoint = factory.createQueue(this.endpointName);
    } else {
      endpoint = factory.createDurableTopicEndpointEx(endpointName);
    }
  }

  protected Consumer getConsumer() throws JCSMPException
  {
    session.provision(endpoint, endpointProperties, endpointProvisionFlags);
    ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
    consumerFlowProperties.setEndpoint(endpoint);
    consumerFlowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
    return session.createFlow(null, consumerFlowProperties, endpointProperties);
  }

  @Override
  public void processMessage(BytesXMLMessage message)
  {
    processGuaranteedMessage(message);
    message.ackMessage();
  }

  protected abstract void processGuaranteedMessage(BytesXMLMessage message);

  public String getEndpointName()
  {
    return endpointName;
  }

  public void setEndpointName(String endpointName)
  {
    this.endpointName = endpointName;
  }

  public EndpointType getEndpointType()
  {
    return endpointType;
  }

  public void setEndpointType(EndpointType endpointType)
  {
    this.endpointType = endpointType;
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
