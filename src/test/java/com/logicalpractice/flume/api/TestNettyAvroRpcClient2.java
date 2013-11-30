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
package com.logicalpractice.flume.api;

import org.apache.avro.ipc.Server;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.NettyAvroRpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import com.logicalpractice.flume.api.RpcTestUtils.FailedAvroHandler;
import com.logicalpractice.flume.api.RpcTestUtils.OKAvroHandler;
import com.logicalpractice.flume.api.RpcTestUtils.ThrowingAvroHandler;
import com.logicalpractice.flume.api.RpcTestUtils.UnknownAvroHandler;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * this is basically a complete copy of the TestNettyAvroRpcClient from the flume source tree.
 */
public class TestNettyAvroRpcClient2 {

  private static final Logger logger = LoggerFactory
      .getLogger(TestNettyAvroRpcClient2.class);

  private static final String localhost = "127.0.0.1";

  /**
   * Simple request
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test
  public void testOKServerSimple() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerSimpleAppendTest(new OKAvroHandler());
  }

  /**
   * Simple request with compression on the server and client with compression level 6
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test
  public void testOKServerSimpleCompressionLevel6() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerSimpleAppendTest(new OKAvroHandler(), true, true, 6);
  }

  /**
   * Simple request with compression on the server and client with compression level 0
   *
   * Compression level 0 = no compression
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test
  public void testOKServerSimpleCompressionLevel0() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerSimpleAppendTest(new OKAvroHandler(), true, true, 0);
  }

  /**
   * Simple request with compression on the client only
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test(expected=org.apache.flume.EventDeliveryException.class)
  public void testOKServerSimpleCompressionClientOnly() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerSimpleAppendTest(new OKAvroHandler(), false, true, 6);
  }

  /**
   * Simple request with compression on the server only
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test(expected=org.apache.flume.EventDeliveryException.class)
  public void testOKServerSimpleCompressionServerOnly() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerSimpleAppendTest(new OKAvroHandler(), true, false, 6);
  }

  /**
   * Simple batch request
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test
  public void testOKServerBatch() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerBatchAppendTest(new OKAvroHandler());
  }

  /**
   * Simple batch request with compression deflate level 0
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test
  public void testOKServerBatchCompressionLevel0() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerBatchAppendTest(new OKAvroHandler(), true, true, 0);
  }

  /**
   * Simple batch request with compression deflate level 6
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test
  public void testOKServerBatchCompressionLevel6() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerBatchAppendTest(new OKAvroHandler(), true, true, 6);
  }

  /**
   * Simple batch request where the server only is using compression
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test(expected=org.apache.flume.EventDeliveryException.class)
  public void testOKServerBatchCompressionServerOnly() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerBatchAppendTest(new OKAvroHandler(), true, false, 6);
  }

  /**
   * Simple batch request where the client only is using compression
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test(expected=org.apache.flume.EventDeliveryException.class)
  public void testOKServerBatchCompressionClientOnly() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerBatchAppendTest(new OKAvroHandler(), false, true, 6);
  }

  /**
   * Try to connect to a closed port.
   * Note: this test tries to connect to port 1 on localhost.
   * @throws org.apache.flume.FlumeException
   */
  @Test(expected=FlumeException.class)
  public void testUnableToConnect() throws FlumeException {
    @SuppressWarnings("unused")
    NettyAvroRpcClient2 client = new NettyAvroRpcClient2(
        NettySocketChannels.newSocketChannelFactory(),
        Executors.newCachedThreadPool());
    try {
      Properties props = new Properties();
      props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "localhost");
      props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "localhost",
          localhost + ":" + 1);
      client.configure(props);
    }finally {
      client.close();
    }
  }

  /**
   * Send too many events at once. Should handle this case gracefully.
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test
  public void testBatchOverrun() throws FlumeException, EventDeliveryException {

    int batchSize = 10;
    int moreThanBatchSize = batchSize + 1;
    NettyAvroRpcClient2 client = null;
    Server server = RpcTestUtils.startServer(new OKAvroHandler());
    Properties props = new Properties();
    props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "localhost");
    props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "localhost",
        localhost + ":" + server.getPort());
    props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, "" + batchSize);
    try {
      client = new NettyAvroRpcClient2(
          NettySocketChannels.newSocketChannelFactory(),
          Executors.newCachedThreadPool()
      );
      client.configure(props);

      // send one more than the batch size
      List<Event> events = new ArrayList<Event>();
      for (int i = 0; i < moreThanBatchSize; i++) {
        events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
      }
      client.appendBatch(events);
    } finally {
      RpcTestUtils.stopServer(server);
      if (client != null) client.close();
    }
  }

  /**
   * First connect the client, then shut down the server, then send a request.
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   * @throws InterruptedException
   */
  @Test(expected=EventDeliveryException.class)
  public void testServerDisconnect() throws FlumeException,
      EventDeliveryException, InterruptedException {
    NettyAvroRpcClient2 client = null;
    Server server = RpcTestUtils.startServer(new OKAvroHandler());
    try {
      client = RpcTestUtils.getStockLocalClient(server.getPort());
      server.close();
      Thread.sleep(1000L); // wait a second for the close to occur
      try {
        server.join();
      } catch (InterruptedException ex) {
        logger.warn("Thread interrupted during join()", ex);
        Thread.currentThread().interrupt();
      }
      try {
        client.append(EventBuilder.withBody("hello", Charset.forName("UTF8")));
      } finally {
        Assert.assertFalse("Client should not be active", client.isActive());
      }
    } finally {
      RpcTestUtils.stopServer(server);
      if (client != null) client.close();
    }
  }

  /**
   * First connect the client, then close the client, then send a request.
   * @throws org.apache.flume.FlumeException
   * @throws org.apache.flume.EventDeliveryException
   */
  @Test(expected=EventDeliveryException.class)
  public void testClientClosedRequest() throws FlumeException,
      EventDeliveryException {
    NettyAvroRpcClient2 client = null;
    Server server = RpcTestUtils.startServer(new OKAvroHandler());
    try {
      client = RpcTestUtils.getStockLocalClient(server.getPort());
      client.close();
      Assert.assertFalse("Client should not be active", client.isActive());
      System.out.println("Yaya! I am not active after client close!");
      client.append(EventBuilder.withBody("hello", Charset.forName("UTF8")));
    } finally {
      RpcTestUtils.stopServer(server);
      if (client != null) client.close();
    }
  }

  /**
   * Send an event to an online server that returns FAILED.
   */
  @Test(expected=EventDeliveryException.class)
  public void testFailedServerSimple() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerSimpleAppendTest(new FailedAvroHandler());
    logger.error("Failed: I should never have gotten here!");
  }

  /**
   * Send an event to an online server that returns UNKNOWN.
   */
  @Test(expected=EventDeliveryException.class)
  public void testUnknownServerSimple() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerSimpleAppendTest(new UnknownAvroHandler());
    logger.error("Unknown: I should never have gotten here!");
  }

  /**
   * Send an event to an online server that throws an exception.
   */
  @Test(expected=EventDeliveryException.class)
  public void testThrowingServerSimple() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerSimpleAppendTest(new ThrowingAvroHandler());
    logger.error("Throwing: I should never have gotten here!");
  }

  /**
   * Send a batch of events to a server that returns FAILED.
   */
  @Test(expected=EventDeliveryException.class)
  public void testFailedServerBatch() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerBatchAppendTest(new FailedAvroHandler());
    logger.error("Failed: I should never have gotten here!");
  }

  /**
   * Send a batch of events to a server that returns UNKNOWN.
   */
  @Test(expected=EventDeliveryException.class)
  public void testUnknownServerBatch() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerBatchAppendTest(new UnknownAvroHandler());
    logger.error("Unknown: I should never have gotten here!");
  }

  /**
   * Send a batch of events to a server that always throws exceptions.
   */
  @Test(expected=EventDeliveryException.class)
  public void testThrowingServerBatch() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerBatchAppendTest(new ThrowingAvroHandler());
    logger.error("Throwing: I should never have gotten here!");
  }
}
