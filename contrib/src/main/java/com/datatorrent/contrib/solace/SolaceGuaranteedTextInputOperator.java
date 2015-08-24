package com.datatorrent.contrib.solace;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.TextMessage;

import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by pramod on 8/21/15.
 */
public class SolaceGuaranteedTextInputOperator extends AbstractSolaceGuaranteedInputOperator
{
  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  protected void processGuaranteedMessage(BytesXMLMessage message)
  {
    output.emit(((TextMessage) message).getText());
  }
}
