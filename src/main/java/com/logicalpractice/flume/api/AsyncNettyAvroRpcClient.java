package com.logicalpractice.flume.api;

import org.apache.avro.ipc.Callback;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.AbstractRpcClient;
import org.apache.flume.api.NettyAvroRpcClient;
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
public class AsyncNettyAvroRpcClient extends AbstractNettyAvroRpcClient {

  private final AsyncRpcClientCallback callback;

  private final TimeoutGenerator timeoutGenerator ;

  private static final Logger logger = LoggerFactory
      .getLogger(NettyAvroRpcClient.class);


  public AsyncNettyAvroRpcClient(
      ChannelFactory socketChannelFactory,
      AsyncRpcClientCallback callback,
      TimeoutGenerator timeoutGenerator) {
    super(socketChannelFactory);
    this.callback = callback;
    this.timeoutGenerator = timeoutGenerator;
  }

  @Override
  public void append(Event event) throws EventDeliveryException {
      append(event, requestTimeout, TimeUnit.MILLISECONDS);
  }

  private void append(Event event, final long timeout, final TimeUnit tu)
      throws EventDeliveryException {

    assertReady();

    final AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    avroEvent.setBody(ByteBuffer.wrap(event.getBody()));
    avroEvent.setHeaders(toCharSeqMap(event.getHeaders()));

    try {
      final AsyncAvroCallback callback = new AsyncAvroCallback(
          Collections.singletonList(event),
          System.nanoTime() + tu.toNanos(timeout));

      avroClient.append(avroEvent, callback);
      timeoutGenerator.registerTimeout(callback);
    } catch (IOException ex) {
      throw new EventDeliveryException(this + ": Handshake timed out after " +
          connectTimeout + " ms", ex);
    }
  }

  @Override
  public void appendBatch(List<Event> events) throws EventDeliveryException {
    appendBatch(events, requestTimeout, TimeUnit.MILLISECONDS);
  }

  private void appendBatch(List<Event> events, long timeout, TimeUnit tu)
      throws EventDeliveryException {

    assertReady();

    Iterator<Event> iter = events.iterator();
    final List<AvroFlumeEvent> avroEvents = new LinkedList<AvroFlumeEvent>();

    // send multiple batches... bail if there is a problem at any time
    while (iter.hasNext()) {
      avroEvents.clear();
      List<Event> thisBatch = new ArrayList<Event>(Math.min(batchSize, events.size()));

      for (int i = 0; i < batchSize && iter.hasNext(); i++) {
        Event event = iter.next();
        AvroFlumeEvent avroEvent = new AvroFlumeEvent();
        avroEvent.setBody(ByteBuffer.wrap(event.getBody()));
        avroEvent.setHeaders(toCharSeqMap(event.getHeaders()));
        avroEvents.add(avroEvent);
        thisBatch.add(event);
      }

      try {
        final AsyncAvroCallback callback = new AsyncAvroCallback(
            thisBatch,
            System.nanoTime() + tu.toNanos(timeout));

        avroClient.appendBatch(avroEvents, callback);
        timeoutGenerator.registerTimeout(callback);

      } catch (IOException ex) {
        throw new EventDeliveryException(this + ": IOException", ex);
      }
    }
  }

  @Override
  protected void connected() throws IOException {
    SpecificRequestor.getRemote(avroClient);
  }

  private class AsyncAvroCallback implements Callback<Status>, Timeout {
    private final List<Event> events;
    private final long timeout ;
    private volatile boolean cancelled ;

    public AsyncAvroCallback(List<Event> events, long timeout) {
      this.events = events;
      this.timeout = timeout;
    }

    @Override
    public void handleResult(Status status) {
      if (status != Status.OK) {
        callback.handleError(events, new EventDeliveryException(this + ": Avro RPC call returned " +
            "Status: " + status));
      } else {
        callback.handleComplete(events);
      }

      cancelTimeout();
    }

    @Override
    public void handleError(Throwable error) {
      setState(ConnState.DEAD);
      callback.handleError(events, new EventDeliveryException(this + ": Avro RPC call failed", error));
      cancelTimeout();
    }

    @Override
    public void run() {
      setState(ConnState.DEAD);
      callback.handleError(events, new EventDeliveryException(this + ": Avro RPC timed out", new TimeoutException()));
    }

    private void cancelTimeout() {
      timeoutGenerator.cancelTimeout(this);
      cancelled = true;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(timeout - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      AsyncAvroCallback other = (AsyncAvroCallback) o;
      return Long.compare(this.timeout, other.timeout);
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }
  }
}
