package com.logicalpractice.flume.api;

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
 *
 */
public class NettyAvroRpcClient2 extends AbstractNettyAvroRpcClient {

  private ExecutorService callTimeoutPool;

  private static final Logger logger = LoggerFactory
      .getLogger(NettyAvroRpcClient2.class);

  /**
   * This constructor is intended to be called from {@link org.apache.flume.api.RpcClientFactory}.
   * A call to this constructor should be followed by call to configure().
   */
  public NettyAvroRpcClient2(SharableChannelFactory socketChannelFactory,
                             ExecutorService callTimeoutPool){
    super(socketChannelFactory);
    this.callTimeoutPool = callTimeoutPool;
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

    Future<Void> handshake;
    try {
      // due to AVRO-1122, avroClient.append() may block
      handshake = callTimeoutPool.submit(new Callable<Void>() {

        @Override
        public Void call() throws Exception {
          avroClient.append(avroEvent, callFuture);
          return null;
        }
      });
    } catch (RejectedExecutionException ex) {
      throw new EventDeliveryException(this + ": Executor error", ex);
    }

    try {
      handshake.get(connectTimeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException ex) {
      throw new EventDeliveryException(this + ": Handshake timed out after " +
          connectTimeout + " ms", ex);
    } catch (InterruptedException ex) {
      throw new EventDeliveryException(this + ": Interrupted in handshake", ex);
    } catch (ExecutionException ex) {
      throw new EventDeliveryException(this + ": RPC request exception", ex);
    } catch (CancellationException ex) {
      throw new EventDeliveryException(this + ": RPC request cancelled", ex);
    } finally {
      if (!handshake.isDone()) {
        handshake.cancel(true);
      }
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

      final CallFuture<Status> callFuture = new CallFuture<Status>();

      Future<Void> handshake;
      try {
        // due to AVRO-1122, avroClient.appendBatch() may block
        handshake = callTimeoutPool.submit(new Callable<Void>() {

          @Override
          public Void call() throws Exception {
            avroClient.appendBatch(avroEvents, callFuture);
            return null;
          }
        });
      } catch (RejectedExecutionException ex) {
        throw new EventDeliveryException(this + ": Executor error", ex);
      }

      try {
        handshake.get(connectTimeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException ex) {
        throw new EventDeliveryException(this + ": Handshake timed out after " +
            connectTimeout + "ms", ex);
      } catch (InterruptedException ex) {
        throw new EventDeliveryException(this + ": Interrupted in handshake",
            ex);
      } catch (ExecutionException ex) {
        throw new EventDeliveryException(this + ": RPC request exception", ex);
      } catch (CancellationException ex) {
        throw new EventDeliveryException(this + ": RPC request cancelled", ex);
      } finally {
        if (!handshake.isDone()) {
          handshake.cancel(true);
        }
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
