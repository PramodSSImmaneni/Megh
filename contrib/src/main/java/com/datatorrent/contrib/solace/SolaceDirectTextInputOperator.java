package com.datatorrent.contrib.solace;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.TextMessage;

import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by pramod on 8/24/15.
 */
public class SolaceDirectTextInputOperator extends AbstractSolaceDirectInputOperator
{
  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  protected void processDirectMessage(BytesXMLMessage message)
  {
    output.emit(((TextMessage) message).getText());
  }
}
