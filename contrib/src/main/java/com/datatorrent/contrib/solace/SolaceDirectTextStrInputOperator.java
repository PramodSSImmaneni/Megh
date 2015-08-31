package com.datatorrent.contrib.solace;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.TextMessage;

import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by pramod on 8/24/15.
 */
public class SolaceDirectTextStrInputOperator extends AbstractSolaceDirectInputOperator<String>
{
  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  protected String convert(BytesXMLMessage message)
  {
    return ((TextMessage) message).getText();
  }

  @Override
  protected void emitTuple(String tuple)
  {
    output.emit(tuple);
  }
}
