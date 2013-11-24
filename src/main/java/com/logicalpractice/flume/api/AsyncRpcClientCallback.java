package com.logicalpractice.flume.api;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

import java.util.List;

/**
 *
 */
public interface AsyncRpcClientCallback {

  void handleComplete(List<Event> events);

  void handleError(List<Event> events, EventDeliveryException failure);
}
