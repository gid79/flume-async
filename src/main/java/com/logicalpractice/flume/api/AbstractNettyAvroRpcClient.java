package com.logicalpractice.flume.api;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.AbstractRpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.jboss.netty.channel.ChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public abstract class AbstractNettyAvroRpcClient extends AbstractRpcClient {

  private static Logger logger = LoggerFactory.getLogger(AbstractNettyAvroRpcClient.class);

  enum ConnState {
    INIT, READY, DEAD
  }

  private SharableChannelFactory socketChannelFactory;

  final ReentrantLock stateLock = new ReentrantLock();

  /**
   * Guarded by {@code stateLock}
   */
  private ConnState connState = ConnState.INIT;

  private InetSocketAddress address;

  private Transceiver transceiver;

  protected AvroSourceProtocol.Callback avroClient;

  protected AbstractNettyAvroRpcClient(SharableChannelFactory socketChannelFactory) {
    this.socketChannelFactory = socketChannelFactory;
  }

  /**
    * This method should only be invoked by the build function
    * @throws org.apache.flume.FlumeException
    */
   private void connect() throws FlumeException {
     connect(connectTimeout, TimeUnit.MILLISECONDS);
   }

  /**
   * Internal only, for now
   * @param timeout
   * @param tu
   * @throws org.apache.flume.FlumeException
   */
  private void connect(long timeout, TimeUnit tu) throws FlumeException {

    try {

      transceiver = new NettyTransceiver(this.address,
          socketChannelFactory.releaseSuppressing(),
          tu.toMillis(timeout));
      avroClient =
          SpecificRequestor.getClient(AvroSourceProtocol.Callback.class,
              transceiver);
      connected();
    } catch (Throwable t) {
      try{
        close();
      } catch(FlumeException ex) {
        // ignore and continue
      }
      if (t instanceof IOException) {
        throw new FlumeException(this + ": RPC connection error", t);
      } else if (t instanceof FlumeException) {
        throw (FlumeException) t;
      } else if (t instanceof Error) {
        throw (Error) t;
      } else {
        throw new FlumeException(this + ": Unexpected exception", t);
      }
    }

    setState(ConnState.READY);
  }

  protected void connected() throws Exception {}

  /**
   * This method should always be used to change {@code connState} so we ensure
   * that invalid state transitions do not occur and that the {@code isIdle}
   * {@link java.util.concurrent.locks.Condition} variable gets signaled reliably.
   * Throws {@code IllegalStateException} when called to transition from CLOSED
   * to another state.
   * @param newState
   */
  protected void setState(ConnState newState) {
    stateLock.lock();
    try {
      if (connState == ConnState.DEAD && connState != newState) {
        throw new IllegalStateException("Cannot transition from CLOSED state.");
      }
      connState = newState;
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * If the connection state != READY, throws {@link org.apache.flume.EventDeliveryException}.
   */
  protected void assertReady() throws EventDeliveryException {
    stateLock.lock();
    try {
      ConnState curState = connState;
      if (curState != ConnState.READY) {
        throw new EventDeliveryException("RPC failed, client in an invalid " +
            "state: " + curState);
      }
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * Helper function to convert a map of String to a map of CharSequence.
   */
  protected static Map<CharSequence, CharSequence> toCharSeqMap(
      Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> charSeqMap =
        new HashMap<CharSequence, CharSequence>();
    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
      charSeqMap.put(entry.getKey(), entry.getValue());
    }
    return charSeqMap;
  }

  @Override
  public boolean isActive() {
    stateLock.lock();
    try {
      return (connState == ConnState.READY);
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * <p>
   * Configure the actual client using the properties.
   * <tt>properties</tt> should have at least 2 params:
   * <p><tt>hosts</tt> = <i>alias_for_host</i></p>
   * <p><tt>alias_for_host</tt> = <i>hostname:port</i>. </p>
   * Only the first host is added, rest are discarded.</p>
   * <p>Optionally it can also have a <p>
   * <tt>batch-size</tt> = <i>batchSize</i>
   * @param properties The properties to instantiate the client with.
   * @return
     */
  @Override
  public synchronized void configure(Properties properties)
      throws FlumeException {
    stateLock.lock();
    try{
      if(connState == ConnState.READY || connState == ConnState.DEAD){
        throw new FlumeException("This client was already configured, " +
            "cannot reconfigure.");
      }
    } finally {
      stateLock.unlock();
    }

    // batch size
    String strBatchSize = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_BATCH_SIZE);
    logger.debug("Batch size string = " + strBatchSize);
    batchSize = RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE;
    if (strBatchSize != null && !strBatchSize.isEmpty()) {
      try {
        int parsedBatch = Integer.parseInt(strBatchSize);
        if (parsedBatch < 1) {
          logger.warn("Invalid value for batchSize: {}; Using default value.", parsedBatch);
        } else {
          batchSize = parsedBatch;
        }
      } catch (NumberFormatException e) {
        logger.warn("Batchsize is not valid for RpcClient: " + strBatchSize +
            ". Default value assigned.", e);
      }
    }

    // host and port
    String hostNames = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_HOSTS);
    String[] hosts = null;
    if (hostNames != null && !hostNames.isEmpty()) {
      hosts = hostNames.split("\\s+");
    } else {
      throw new FlumeException("Hosts list is invalid: " + hostNames);
    }

    if (hosts.length > 1) {
      logger.warn("More than one hosts are specified for the default client. "
          + "Only the first host will be used and others ignored. Specified: "
          + hostNames + "; to be used: " + hosts[0]);
    }

    String host = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX+hosts[0]);
    if (host == null || host.isEmpty()) {
      throw new FlumeException("Host not found: " + hosts[0]);
    }
    String[] hostAndPort = host.split(":");
    if (hostAndPort.length != 2){
      throw new FlumeException("Invalid hostname: " + hosts[0]);
    }
    Integer port = null;
    try {
      port = Integer.parseInt(hostAndPort[1]);
    } catch (NumberFormatException e) {
      throw new FlumeException("Invalid Port: " + hostAndPort[1], e);
    }
    this.address = new InetSocketAddress(hostAndPort[0], port);

    // connect timeout
    connectTimeout =
        RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS;
    String strConnTimeout = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_CONNECT_TIMEOUT);
    if (strConnTimeout != null && strConnTimeout.trim().length() > 0) {
      try {
        connectTimeout = Long.parseLong(strConnTimeout);
        if (connectTimeout < 1000) {
          logger.warn("Connection timeout specified less than 1s. " +
              "Using default value instead.");
          connectTimeout =
              RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS;
        }
      } catch (NumberFormatException ex) {
        logger.error("Invalid connect timeout specified: " + strConnTimeout);
      }
    }

    // request timeout
    requestTimeout =
        RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS;
    String strReqTimeout = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT);
    if  (strReqTimeout != null && strReqTimeout.trim().length() > 0) {
      try {
        requestTimeout = Long.parseLong(strReqTimeout);
        if (requestTimeout < 1000) {
          logger.warn("Request timeout specified less than 1s. " +
              "Using default value instead.");
          requestTimeout =
              RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS;
        }
      } catch (NumberFormatException ex) {
        logger.error("Invalid request timeout specified: " + strReqTimeout);
      }
    }

    this.connect();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " { host: " + address.getHostName() + ", port: " +
        address.getPort() + " }";
  }

  public synchronized void close() {
    try {
      if( transceiver != null ) {
        transceiver.close();
        transceiver = null;
      }
    } catch (IOException ex) {
      throw new FlumeException(this + ": Error closing transceiver.", ex);
    } finally {
      setState(ConnState.DEAD);
    }
  }
}
