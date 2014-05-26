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

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.*;
import org.apache.flume.util.OrderSelector;
import org.apache.flume.util.RandomOrderSelector;
import org.apache.flume.util.RoundRobinOrderSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;


/**
 * <p>An implementation of RpcClient interface that uses NettyAvroRpcClient2
 * instances to load-balance the requests over many different hosts. This
 * implementation supports a round-robin scheme or random scheme of doing
 * load balancing over the various hosts. To specify round-robin scheme set
 * the value of the configuration property <tt>load-balance-type</tt> to
 * <tt>round_robin</tt>. Similarly, for random scheme this value should be
 * set to <tt>random</tt>, and for a custom scheme the full class name of
 * the class that implements the <tt>HostSelector</tt> interface.
 * </p>
 * <p>
 * This implementation also performs basic failover in case the randomly
 * selected host is not available for receiving the event.
 * </p>
 */
public class NettyLoadBalancingRpcClient extends AbstractRpcClient {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(NettyLoadBalancingRpcClient.class);

  private final ExecutorService connectPool = Executors.newCachedThreadPool(
      new PrefixedDaemonThreadFactory("Avro client connect"));

  private SharableChannelFactory channelFactory ;

  private final List<ClientHolder> clients = new ArrayList<ClientHolder>();
  private HostSelector selector;
  private Properties configurationProperties;
  private volatile boolean isOpen = false;

  @Override
  public void append(Event event) throws EventDeliveryException {
    throwIfClosed();
    List<ClientHolder> empties = new ArrayList<ClientHolder>(clients.size());

    boolean eventSent = tryAppend(event, selector.createHostIterator(), empties);

    if (!eventSent) {
      eventSent = tryAppend(event, nextAvailableClientIterator(empties), new ArrayList<ClientHolder>());
      if( !eventSent )
        throw new EventDeliveryException("Unable to send event to any host");
    }
  }

  private boolean tryAppend(
      Event event,
      Iterator<ClientHolder> iterator,
      List<ClientHolder> empties) {
    boolean eventSent = false;

    while (iterator.hasNext()) {
      ClientHolder holder = iterator.next();
      try {
        RpcClient client = holder.tryClient();
        if( client != null ) {
          client.append(event);
          eventSent = true;
          break;
        } else {
          empties.add( holder );
        }
      } catch (Exception ex) {
        holder.markFailure();
        LOGGER.warn("Failed to send batch to host " + holder.hostInfo, ex);
      }
    }
    return eventSent;
  }

  private Iterator<ClientHolder> nextAvailableClientIterator(
      final List<ClientHolder> clients
  ) {
    return new AbstractIterator<ClientHolder>() {
      @Override
      protected ClientHolder computeNext() {
        long timeout = TimeUnit.MILLISECONDS.toNanos(connectTimeout);

        while( !clients.isEmpty() && timeout > 0 ) {
          long start = System.nanoTime();
          for (Iterator<ClientHolder> iterator = clients.iterator(); iterator.hasNext(); ) {
            ClientHolder holder = iterator.next();
            try {
              if( holder.awaitClient(1, TimeUnit.MILLISECONDS) != null ){
                iterator.remove();
                return holder; // found connected client
              }
            } catch (TimeoutException e) {
              // ignore ... will move on to the next
            } catch (Exception e) {
              selector.informFailure(holder); // bang remove
              iterator.remove();
            }
          }
          timeout -= System.nanoTime() - start;
        }
        return endOfData();
      }
    };
  }

  @Override
  public void appendBatch(List<Event> events) throws EventDeliveryException {
    throwIfClosed();

    ArrayList<ClientHolder> empties = new ArrayList<ClientHolder>();

    boolean batchSent = tryAppendBatch(events, selector.createHostIterator(), empties);

    if (!batchSent) {
      batchSent  = tryAppendBatch(events, nextAvailableClientIterator(empties), empties);
      if( !batchSent )
        throw new EventDeliveryException("Unable to send batch to any host");
    }
  }

  private boolean tryAppendBatch(
      List<Event> events,
      Iterator<ClientHolder> iterator,
      List<ClientHolder> empties) {
    boolean batchSent = false;

    while (iterator.hasNext()) {
      ClientHolder holder = iterator.next();
      try {
        RpcClient client = holder.tryClient();
        if( client != null ) {
          client.appendBatch(events);
          batchSent = true;
          break;
        } else {
          empties.add( holder );
        }
      } catch (Exception ex) {
        holder.markFailure();
        LOGGER.warn("Failed to send batch to host " + holder.hostInfo, ex);
      }
    }
    return batchSent;
  }

  @Override
  public boolean isActive() {
    return isOpen;
  }

  public void awaitConnected() {
    List<ClientHolder> clients = new ArrayList<ClientHolder>(this.clients);
    Iterator<ClientHolder> it = nextAvailableClientIterator(clients);
    while( it.hasNext() )
      it.next();
  }

  private void throwIfClosed() throws EventDeliveryException {
    if (!isOpen) {
      throw new EventDeliveryException("Rpc Client is closed");
    }
  }

  @Override
  public void close() throws FlumeException {
    isOpen = false;
    synchronized (this) {
      for (ClientHolder client : clients) {
        try {
          client.close();
        } catch (Exception ex) {
          LOGGER.warn("Failed to close client: " + client.hostInfo, ex);
        }
      }
    }
    if( channelFactory != null )
      channelFactory.releaseExternalResources();
  }

  @Override
  protected synchronized void configure(Properties properties) throws FlumeException {
    configurationProperties = new Properties();
    configurationProperties.putAll(properties);

    String lbTypeName = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_HOST_SELECTOR,
        RpcClientConfigurationConstants.HOST_SELECTOR_ROUND_ROBIN);

    boolean backoff = Boolean.valueOf(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_BACKOFF,
        String.valueOf(false)));

    String maxBackoffStr = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_MAX_BACKOFF);

    long maxBackoff = 0;
    if(maxBackoffStr != null) {
      maxBackoff = Long.parseLong(maxBackoffStr);
    }

    if (lbTypeName.equalsIgnoreCase(
        RpcClientConfigurationConstants.HOST_SELECTOR_ROUND_ROBIN)) {
      selector = new RoundRobinHostSelector(backoff, maxBackoff);
    } else if (lbTypeName.equalsIgnoreCase(
        RpcClientConfigurationConstants.HOST_SELECTOR_RANDOM)) {
      selector = new RandomOrderHostSelector(backoff, maxBackoff);
    } else {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends HostSelector> klass = (Class<? extends HostSelector>)
            Class.forName(lbTypeName);

        selector = klass.newInstance();
      } catch (Exception ex) {
        throw new FlumeException("Unable to instantiate host selector: "
            + lbTypeName, ex);
      }
    }

    channelFactory = NettySocketChannels.newSocketChannelFactory(
        NettyConfiguration.fromProperties(properties).build()
    );

    List<HostInfo> hosts = HostInfo.getHostInfoList(properties);

    for (HostInfo host : hosts) {
      clients.add(new ClientHolder(host)); // will initiate the first connects
    }
    selector.setHosts(clients);

    isOpen = true;
  }

  private Properties getClientConfigurationProperties(String referenceName) {
    Properties props = new Properties();
    props.putAll(configurationProperties);
    props.put(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
        RpcClientFactory.ClientType.DEFAULT);
    props.put(RpcClientConfigurationConstants.CONFIG_HOSTS, referenceName);

    return props;
  }

  public interface HostSelector {

    void setHosts(List<ClientHolder> hosts);

    Iterator<ClientHolder> createHostIterator();

    void informFailure(ClientHolder failedHost);
  }

  /**
   * A host selector that implements the round-robin host selection policy.
   */
  private static class RoundRobinHostSelector implements HostSelector {

    private final OrderSelector<ClientHolder> selector;

    RoundRobinHostSelector(boolean backoff, long maxBackoff){
      selector = new RoundRobinOrderSelector<ClientHolder>(backoff);
      if(maxBackoff != 0){
        selector.setMaxTimeOut(maxBackoff);
      }
    }
    @Override
    public synchronized Iterator<ClientHolder> createHostIterator() {
      return selector.createIterator();
    }

    @Override
    public synchronized void setHosts(List<ClientHolder> hosts) {
      selector.setObjects(hosts);
    }

    public synchronized void informFailure(ClientHolder failedHost){
      selector.informFailure(failedHost);
    }
  }

  private static class RandomOrderHostSelector implements HostSelector {

    private final OrderSelector<ClientHolder> selector;

    RandomOrderHostSelector(boolean backoff, Long maxBackoff) {
      selector = new RandomOrderSelector<ClientHolder>(backoff);
      if (maxBackoff != 0) {
        selector.setMaxTimeOut(maxBackoff);
      }
    }

    @Override
    public synchronized Iterator<ClientHolder> createHostIterator() {
      return selector.createIterator();
    }

    @Override
    public synchronized void setHosts(List<ClientHolder> hosts) {
      selector.setObjects(hosts);
    }

    @Override
    public void informFailure(ClientHolder failedHost) {
      selector.informFailure(failedHost);
    }
  }

  private class ClientHolder {
    private final HostInfo hostInfo;
    private final AtomicReference<Future<RpcClient>> ref;

    private ClientHolder(HostInfo hostInfo) {
      this.hostInfo = hostInfo;
      this.ref = new AtomicReference<Future<RpcClient>>(connect(hostInfo));
    }

    public RpcClient tryClient() throws Exception {
      throwIfClosed();
      Future<RpcClient> theFuture = this.ref.get();
      if( theFuture.isDone() ){
        // note since the future is done the timeout doesn't matter
        return get(theFuture, 0, TimeUnit.NANOSECONDS);
      } else {
        return null; // not ready yet
      }
    }

    public RpcClient awaitClient(long maxwait, TimeUnit unit) throws Exception {
      throwIfClosed();
      return get(ref.get(), maxwait, unit);
    }

    private RpcClient get(
        Future<RpcClient> theFuture,
        long maxwait,
        TimeUnit unit) throws Exception {
      try {
        RpcClient client = theFuture.get(maxwait, unit);
        if( client.isActive() )
          return client;
        else {
          client.close();
          tryReconnect(theFuture);
          return null; // skip
        }
      } catch (InterruptedException e) {
        // really shouldn't happen as there is zero or very little blocking
        Thread.currentThread().interrupt();
        return null;
      } catch (ExecutionException e) {
        tryReconnect(theFuture);
        Throwables.propagateIfInstanceOf(e.getCause(), Exception.class);
        throw Throwables.propagate(e.getCause());
      }
    }

    private synchronized void tryReconnect(Future<RpcClient> theFuture) {
      FutureTask<RpcClient> attempt = new FutureTask<RpcClient>(newConnection(hostInfo));
      if( this.ref.compareAndSet(theFuture, attempt)) {
        connectPool.submit(attempt);
      } // else somebody else has already got here
    }

    public void close() {
      Future<RpcClient> theFuture = ref.get();
      if( theFuture.cancel(true) ){
        try {
          RpcClient client = get(theFuture, connectTimeout, TimeUnit.MILLISECONDS);
          client.close();
        } catch (Exception e) {
          // ignore
        }
      }
    }

    public void markFailure() {
      try {
        tryClient();
      } catch (Exception e) {
        // ignore completely, it's actually quite unlikely to explode here
        // must likely is tryClient to return a null having kicked off a reconnect
        // and closed the client as it'll be !alive
      }
      selector.informFailure(this);
    }
  }

  private Future<RpcClient> connect(final HostInfo hostInfo) {
    return connectPool.submit(newConnection(hostInfo));
  }

  private Callable<RpcClient> newConnection(final HostInfo hostInfo) {
    return new Callable<RpcClient>() {
      @Override
      public RpcClient call() throws Exception {
        NettyAvroRpcClient2 client = new NettyAvroRpcClient2(
            channelFactory
        );
        try {
          client.configure(getClientConfigurationProperties(hostInfo.getReferenceName()));
        } catch (Exception e) {
          client.close();
          throw e;
        }
        return client;
      }
    };
  }
}

