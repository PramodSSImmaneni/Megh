package com.datatorrent.contrib.solace;

import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;

/**
 * Created by pramod on 10/2/15.
 */
public class SolaceGuaranteedStrTextOutputOperator extends AbstractSolaceGuaranteedOutputOperator<String>
{
  @Override
  protected XMLMessage convert(String tuple)
  {
    TextMessage textMessage = factory.createMessage(TextMessage.class);
    textMessage.setText(tuple);
    return textMessage;
  }
}
