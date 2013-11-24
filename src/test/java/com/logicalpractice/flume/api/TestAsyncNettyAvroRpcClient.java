package com.logicalpractice.flume.api;

import org.apache.avro.ipc.Server;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestAsyncNettyAvroRpcClient {

  public static final Charset UTF8 = Charset.forName("UTF8");

  @Test
  public void simpleAppend() throws Exception {
    RpcTestUtils.OKAvroHandler handler = new RpcTestUtils.OKAvroHandler();
    Server server = RpcTestUtils.startServer(handler);
    try {
      AsyncNettyAvroRpcClient client = RpcTestUtils.getStockAsyncLocalClient(server.getPort());
      boolean isActive = client.isActive();
      assertTrue("Client should be active", isActive);
      client.append(EventBuilder.withBody("wheee!!!", UTF8));

      AvroFlumeEvent event = handler.events().poll(500, TimeUnit.MILLISECONDS);
      assertThat("Unexpected null event, must have timed out",event, notNullValue());
      assertThat(new String(event.getBody().array(), UTF8), equalTo("wheee!!!"));
    } finally {
      server.close();
    }
  }
}
