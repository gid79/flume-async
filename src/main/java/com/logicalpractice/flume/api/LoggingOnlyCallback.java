package com.logicalpractice.flume.api;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Default Async callback used if
 */
public class LoggingOnlyCallback implements AsyncRpcClientCallback {

  private final static Logger logger = LoggerFactory.getLogger(LoggingOnlyCallback.class);

  @Override
  public void handleComplete(List<Event> events) {
    logger.info("completed: {} events", events.size());
  }

  @Override
  public void handleError(List<Event> events, EventDeliveryException failure) {
    logger.warn("failed to deliver: {} events", events.size(), failure);
  }
}
