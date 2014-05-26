package com.logicalpractice.flume.api;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.AbstractRpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Direct replacement for NettyAvroRpcClient from the flume-sdk.
 */
public class NettyAvroRpcClient2 extends AbstractNettyAvroRpcClient {

  private static final Logger logger = LoggerFactory
      .getLogger(NettyAvroRpcClient2.class);

  /**
   * This constructor is intended to be called from {@link com.logicalpractice.flume.api.NettyLoadBalancingRpcClient}.
   * A call to this constructor should be followed by call to configure().
   */
  public NettyAvroRpcClient2(SharableChannelFactory socketChannelFactory){
    super(socketChannelFactory);
  }

  @Override
  public void append(Event event) throws EventDeliveryException {
    try {
      append(event, requestTimeout, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      // we mark as no longer active without trying to clean up resources
      // client is required to call close() to clean up resources
      setState(ConnState.DEAD);
      if (t instanceof Error) {
        throw (Error) t;
      }
      if (t instanceof EventDeliveryException) {
        throw (EventDeliveryException)t;
      }
      throw new EventDeliveryException(this + ": Failed to send event", t);
    }
  }

  private void append(Event event, long timeout, TimeUnit tu)
      throws EventDeliveryException {

    assertReady();

    final CallFuture<Status> callFuture = new CallFuture<Status>();

    final AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    avroEvent.setBody(ByteBuffer.wrap(event.getBody()));
    avroEvent.setHeaders(toCharSeqMap(event.getHeaders()));

    try {
      avroClient.append(avroEvent, callFuture);
    } catch (IOException e) {
      throw new EventDeliveryException("IOException caught sending :" + event, e);
    }

    waitForStatusOK(callFuture, timeout, tu);
  }

  @Override
  public void appendBatch(List<Event> events) throws EventDeliveryException {
    try {
      appendBatch(events, requestTimeout, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      // we mark as no longer active without trying to clean up resources
      // client is required to call close() to clean up resources
      setState(ConnState.DEAD);
      if (t instanceof Error) {
        throw (Error) t;
      }
      if (t instanceof EventDeliveryException) {
        throw (EventDeliveryException)t;
      }
      throw new EventDeliveryException(this + ": Failed to send batch", t);
    }
  }

  private void appendBatch(List<Event> events, long timeout, TimeUnit tu)
      throws EventDeliveryException {

    assertReady();

    Iterator<Event> iter = events.iterator();
    final List<AvroFlumeEvent> avroEvents = new LinkedList<AvroFlumeEvent>();

    // send multiple batches... bail if there is a problem at any time
    while (iter.hasNext()) {
      avroEvents.clear();

      for (int i = 0; i < batchSize && iter.hasNext(); i++) {
        Event event = iter.next();
        AvroFlumeEvent avroEvent = new AvroFlumeEvent();
        avroEvent.setBody(ByteBuffer.wrap(event.getBody()));
        avroEvent.setHeaders(toCharSeqMap(event.getHeaders()));
        avroEvents.add(avroEvent);
      }

      final CallFuture<Status> callFuture = new CallFuture<>();

      try {
        avroClient.appendBatch(avroEvents, callFuture);
      } catch (IOException e) {
        throw new EventDeliveryException("IOException caught sending :" + events, e);
      }

      waitForStatusOK(callFuture, timeout, tu);
    }
  }

  /**
   * Helper method that waits for a Status future to come back and validates
   * that it returns Status == OK.
   * @param callFuture Future to wait on
   * @param timeout Time to wait before failing
   * @param tu Time Unit of {@code timeout}
   * @throws EventDeliveryException If there is a timeout or if Status != OK
   */
  private void waitForStatusOK(CallFuture<Status> callFuture,
      long timeout, TimeUnit tu) throws EventDeliveryException {
    try {
      Status status = callFuture.get(timeout, tu);
      if (status != Status.OK) {
        throw new EventDeliveryException(this + ": Avro RPC call returned " +
            "Status: " + status);
      }
    } catch (CancellationException ex) {
      throw new EventDeliveryException(this + ": RPC future was cancelled", ex);
    } catch (ExecutionException ex) {
      throw new EventDeliveryException(this + ": Exception thrown from " +
          "remote handler", ex);
    } catch (TimeoutException ex) {
      throw new EventDeliveryException(this + ": RPC request timed out", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new EventDeliveryException(this + ": RPC request interrupted", ex);
    }
  }
}
