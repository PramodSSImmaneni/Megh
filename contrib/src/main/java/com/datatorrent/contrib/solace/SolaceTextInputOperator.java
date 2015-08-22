package com.datatorrent.contrib.solace;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.TextMessage;

import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by pramod on 8/21/15.
 */
public class SolaceTextInputOperator extends AbstractSolaceInputOperator
{
  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  protected void processMessage(BytesXMLMessage message)
  {
    if (message instanceof TextMessage) {
      output.emit(((TextMessage) message).getText());
    }
  }
}
