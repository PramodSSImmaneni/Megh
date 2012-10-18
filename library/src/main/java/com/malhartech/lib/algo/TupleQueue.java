/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.Module;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.OperatorConfiguration;
import com.malhartech.api.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port <b>data</b>. The data is key, value pair. It retains the last N values on that key.
 * Output port gets the last N values. The node also provides a lookup via port <b>lookup</b>
 * <br>
 * <br>
 * <b>Schema</b>:
 * Input port "data": The incoming tuple have to be HashMap<String, Object>. Strings are key and Object is value<br>
 * Input port "query": The incoming tuple has to be String. This is the key on which a query is done<br>
 * Output port "queue": Sends out Object class<br>
 * Output port "console": Sends out HashMap<String, ArrayList>. String is the key, and ArrayList is the in order list of Objects
 * <br>
 * <b>Description</b>: Takes data for every key and keeps last N values. N is the value given by property <b>depth</b>
 * <br>
 * <b>Benchmarks</b>: The benchmarks are done by blasting as many HashMaps as possible on inline mode<br>
 * <br>
 * <b>Port Interface</b>:
 * data: Input data as HashMap<String, Object>. This is the key value pair that is inserted into the QUEUE<br>
 * query: Special input port that allows operation of QUEUE module to respond to a query. The query is a String object that is the key on which to query. Output is sent to console port<br>
 * queue: Output of queue for every key. Sends the ejected Object<br>
 * console: Sends out all the queue contents for the String on the query port<br>
 * <br>
 * <b>Properties</b>
 * depth: Depth of the queue. The number of objects to be retained<br>
 *
 *
 * @author amol
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = TupleQueue.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = TupleQueue.IPORT_QUERY, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = TupleQueue.OPORT_QUEUE, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = TupleQueue.OPORT_CONSOLE, type = PortAnnotation.PortType.OUTPUT)
})
public class TupleQueue extends Module
{
  public static final String IPORT_QUERY = "query";
  public static final String IPORT_DATA = "data";
  public static final String OPORT_QUEUE = "queue";
  public static final String OPORT_CONSOLE = "console";
  private static Logger LOG = LoggerFactory.getLogger(TupleQueue.class);

  HashMap<String, ValueData> vmap = new HashMap<String, ValueData>();
  boolean queue_connected = false;
  boolean console_connected = false;

  final int depth_default = 10;
  int depth = depth_default;

  HashMap<String, Object> queryHash = new HashMap<String, Object>();

  /**
   * Depth of the QUEUE
   */
  public static final String KEY_DEPTH = "depth";


  class ValueData
  {
    int index = 0;
    ArrayList list = new ArrayList();

    ValueData(Object o) {
      list.add(o);
      index++;
    }

    /**
     * Inserts Object at the tail of the queue
     * @param val
     * @return Object: the Object at the top of the queue after it is full
     */
    public Object insert(Object val, int depth) {
      Object ret = null;
      if (list.size() >= depth) {
        if (index >= depth) { //rollover to start
          index = 0;
        }
        ret = list.get(index);
        list.set(index, val);
        index++;
      }
      else {
        list.add(val);
        index++;
      }
       return ret;
    }

    public ArrayList getList(int depth) {
      ArrayList ret = new ArrayList();
      if (list.size() >= depth) { // list is full
        int i = index;
        while (i < depth) {
          ret.add(list.get(i));
          i++;
        }
        i = 0;
        while (i < index) {
          ret.add(list.get(i));
          i++;
        }
      }
      else { // not yet fully filled up
        for (int i = 0; i < index; i++) {
          ret.add(list.get(i));
        }
      }
      return ret;
    }
  }

  /**
   *
   * @param id
   * @param dagpart
   */
  @Override
  public void connected(String id, Sink dagpart)
  {
    if (id.equals(OPORT_QUEUE)) {
      queue_connected = (dagpart != null);
    }
    else if (id.equals(OPORT_CONSOLE)) {
      console_connected = (dagpart != null);
    }
  }


  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    if (IPORT_DATA.equals(getActivePort())) {
      for (Map.Entry<String, Object> e: ((HashMap<String, Object>)payload).entrySet()) {
        String key = e.getKey();
        ValueData val = vmap.get(key);
        if (val == null) {
          val = new ValueData(e.getValue());
          vmap.put(key, val);
        }
        else {
          Object ret = val.insert(e.getValue(), depth);
          if (queue_connected) {
            if (ret != null) { // means something popped out of the queue
              HashMap<String, Object> tuple = new HashMap<String, Object>();
              tuple.put(key, ret);
              emit(OPORT_QUEUE, tuple);
            }
          }
        }
      }
    }
    else if (console_connected) { // Query port, no point processing if console is not connected
      String key = (String) payload;
      queryHash.put(key, new Object());
      emitConsoleTuple(key);
    }
  }

  void emitConsoleTuple(String key)
  {
    ValueData val = vmap.get(key);
    ArrayList list;
    HashMap<String, ArrayList> tuple = new HashMap<String, ArrayList>();
    if (val != null) {
      list = val.getList(depth);
    }
    else {
      list = new ArrayList(); // If no data, send an empty ArrayList
    }
    tuple.put(key, list);
    emit(OPORT_CONSOLE, tuple);
  }


  @Override
  public void endWindow() {
      for (Map.Entry<String, Object> e: ((HashMap<String, Object>) queryHash).entrySet()) {
        emitConsoleTuple(e.getKey());
      }
  }

  //
  // Need to add emiting last console tuple on every end_window
  //

  public boolean myValidation(OperatorConfiguration config)
  {
    boolean ret = true;

    try {
      depth = config.getInt(KEY_DEPTH, 1);
    }
    catch (Exception e) {
      ret = false;
      throw new IllegalArgumentException(String.format("key %s (%s) has to be an integer",
                                                       KEY_DEPTH, config.get(KEY_DEPTH)));
    }
     return ret;
  }

    /**
   *
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("validation failed");
    }

    depth = config.getInt(KEY_DEPTH, depth_default);
    LOG.debug(String.format("Set depth to %d)", depth));
  }

  /**
   *
   * Checks for user specific configuration values<p>
   *
   * @param config
   * @return boolean
   */
  @Override
  public boolean checkConfiguration(OperatorConfiguration config)
  {
    boolean ret = true;
    // TBD
    return ret && super.checkConfiguration(config);
  }
}
