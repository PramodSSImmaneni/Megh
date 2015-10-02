package com.datatorrent.contrib.solace;

import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;

import com.datatorrent.api.DefaultInputPort;

/**
 * Created by pramod on 10/2/15.
 */
public class SolaceDirectStrTextOutputOperator extends AbstractSolaceDirectOutputOperator<String>
{

  public transient final DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      sendMessage(s);
    }
  };

  @Override
  protected XMLMessage convert(String tuple)
  {
    TextMessage textMessage = factory.createMessage(TextMessage.class);
    textMessage.setText(tuple);
    return textMessage;
  }
}
