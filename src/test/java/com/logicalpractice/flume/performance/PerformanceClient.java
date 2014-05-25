package com.logicalpractice.flume.performance;

import ch.qos.logback.classic.LoggerContext;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.logicalpractice.flume.api.NettyLoadBalancingRpcClient;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 *
 */
public class PerformanceClient {

  public static void main(String[] args) throws ArgumentParserException, InterruptedException {
    ArgumentParser parser = buildArgumentParser();

    Namespace ns = null;
    try {
        ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
        parser.handleError(e);
        System.exit(1);
    }
    configureLogging(ns);

    Logger logger = (Logger) LoggerFactory.getLogger(PerformanceClient.class);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    logger.info("{}", ns);
    int threads = ns.getInt("threads");
    int clients = ns.getInt("clients");
    CountDownLatch latch = new CountDownLatch(threads * clients + 1);
    ExecutorService executor = Executors.newFixedThreadPool(threads * clients);

    TaskConfiguration configuration = new TaskConfiguration(
        ns.getInt("iterations"),
        ns.getInt("batch_size"),
        ns.getInt("message_size")
    );
    MetricRegistry registry = new MetricRegistry();
    ClientMetrics metrics = new ClientMetrics(registry);
    List<RpcClient> allClients = Lists.newLinkedList();
    try{
      for( int client = 0; client < clients ; client ++ ) {
        RpcClient rpcClient = buildClient(ns);
        logger.info("{}", rpcClient.getClass().getName());

        for( int thread = 0; thread < threads; thread ++ ) {
          executor.submit(new ContinuousRun(latch, rpcClient, metrics, configuration));
        }
        allClients.add(rpcClient);
      }

      logger.info("Beginning the run: clients:" + clients +
          ", threads:" + threads +
          ", iterations:" + ns.getInt("iterations") +
          ", batchSize:" + ns.getInt("batch_size") +
          ", messageSize:" + ns.getInt("message_size"));
      ScheduledFuture<?> metricsFuture = scheduler.scheduleAtFixedRate(metrics.reporter(),1 , 1 , TimeUnit.SECONDS);
      latch.countDown();
      latch.await();

      executor.shutdown();
      executor.awaitTermination(24, TimeUnit.HOURS);
      metricsFuture.cancel(false);
      scheduler.shutdown();
      logger.info("Test has finished :{}", metrics);
    } finally {

      for (RpcClient client : allClients) {
        try {
          client.close();
        } catch (FlumeException e) {
          // ignore
        }
      }
    }
  }

  private static RpcClient buildClient(Namespace ns) {
    Properties props = new Properties();
    props.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE, ns.getString("client_type"));
    List<String> hosts = ns.getList("host");
    List<String> hostNames = new ArrayList<>();
    for (int i = 0; i < hosts.size(); i++) {
      String name = "h" + i;
      hostNames.add(name);
      props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + name, hosts.get(i));
    }
    props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, Joiner.on(' ').join(hostNames));

    if( ns.getBoolean("enable_backoff") ) {
      props.setProperty(RpcClientConfigurationConstants.CONFIG_BACKOFF, "true");
      props.setProperty(RpcClientConfigurationConstants.CONFIG_MAX_BACKOFF, ns.getInt("max_backoff").toString());
    }
//    System.out.println(props.toString());
    return RpcClientFactory.getInstance(props);
  }

  private static void configureLogging(Namespace ns) {
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    Logger root = lc.getLogger(Logger.ROOT_LOGGER_NAME);
    if( ns.getBoolean("verbose") ){
      root.setLevel(Level.DEBUG);
    } else {
      root.setLevel(Level.ERROR);
      lc.getLogger(PerformanceClient.class).setLevel(Level.INFO);
    }
  }

  private static ArgumentParser buildArgumentParser() {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("PerformanceClient");

    parser.addArgument("--host")
        .type(String.class)
        .nargs("*")
        .setDefault(Lists.newArrayList("localhost:2001"));

    parser.addArgument("-t","--threads")
        .type(Integer.class)
        .setDefault(1);

    parser.addArgument("-c","--clients")
        .type(Integer.class)
        .setDefault(1);

    parser.addArgument("--client-type")
        .type(String.class)
        .setDefault(NettyLoadBalancingRpcClient.class.getName());

    parser.addArgument("-v","--verbose")
        .action(storeTrue())
        .setDefault(false)
        ;

    parser.addArgument("-i","--iterations")
        .type(Integer.class)
        .setDefault(10000);

    parser.addArgument("-b","--batch-size")
        .type(Integer.class)
        .setDefault(10);

    parser.addArgument("-s","--message-size")
        .type(Integer.class)
        .setDefault(500);

    parser.addArgument("--enable-backoff")
        .type(Boolean.class)
        .action(storeTrue())
        .setDefault(false);

    parser.addArgument("--max-backoff")
        .type(Integer.class)
        .setDefault(30000);

    return parser;
  }

  static class TaskConfiguration {
    private final int iterations;
    private final int batchSize ;
    private final int messageSize;

    TaskConfiguration(int iterations, int batchSize, int messageSize) {
      this.iterations = iterations;
      this.batchSize = batchSize;
      this.messageSize = messageSize;
    }

    public int getIterations() {
      return iterations;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public int getMessageSize() {
      return messageSize;
    }
  }

  static class ClientMetrics {

    private final Histogram appendBatch;
    private final Meter appendBatchRate;
    private final Meter errorRate;

    ClientMetrics(MetricRegistry registry) {
      appendBatch = registry.histogram("appendBatch");
      appendBatchRate = registry.meter("appendBatchRate");
      errorRate = registry.meter("errors");
    }

    public void markAppendBatch(long timeNs) {
      appendBatch.update(timeNs);
      appendBatchRate.mark();
    }

    public void markError(){ errorRate.mark(); }

    @Override
    public String toString() {
      Snapshot ss = appendBatch.getSnapshot();
      return "Response times: " +
          String.format("50th:%5.3fms, 75th:%5.3fms, 95th:%5.3fms, 99th:%5.3fms",
              millis(ss.getMean()),
              millis(ss.get75thPercentile()),
              millis(ss.get95thPercentile()),
              millis(ss.get99thPercentile())) +
          ", Count:" + appendBatch.getCount() +
          ", Rate:" + String.format("%.1f", appendBatchRate.getOneMinuteRate()) + "/s" +
          ", Errors: " + errorRate.getCount() + " at "
          + String.format("%.1f", errorRate.getOneMinuteRate()) + "/s" ;

    }
    public double millis(double nanos) {
      return nanos / 1000000.0;
    }

    public Runnable reporter() {
      final Logger logger = (Logger) LoggerFactory.getLogger(ClientMetrics.class);
      return new Runnable(){
        @Override
        public void run() {
          logger.info(ClientMetrics.this.toString());
        }
      };
    }
  }

  static abstract class ClientRunnable implements Runnable {
    private Logger logger = (Logger) LoggerFactory.getLogger(ClientRunnable.class);
    private final CountDownLatch latch;
    private final RpcClient client;
    private final ClientMetrics metrics;
    private final TaskConfiguration configuration;
    private byte[] messageBody;

    ClientRunnable(CountDownLatch latch, RpcClient client, ClientMetrics metrics, TaskConfiguration configuration) {
      this.latch = latch;
      this.client = client;
      this.metrics = metrics;
      this.configuration = configuration;
      try {
        this.messageBody = RandomStringUtils.randomAscii(configuration.getMessageSize()).getBytes("US-ASCII");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public final void run() {
      try {
        latch.countDown();
        latch.await();
        doRun();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.info("ClientRunnable interrupted");
      }
    }

    protected void doRun() {
      for (int i = 0; i < configuration.getIterations(); i++) {
        doEach();
      }
    }

    protected abstract void doEach();

    protected TaskConfiguration configuration() {
      return configuration;
    }

    protected void appendBatch(int count) {
      List<Event> events = new ArrayList<Event>(count);
      for (int i = 0; i < count; i++) {
        events.add(EventBuilder.withBody(
            messageBody
        ));
      }

      long start = System.nanoTime();
      try {
        client.appendBatch(events);
      } catch (EventDeliveryException e) {
        metrics.markError();
      } finally {
        metrics.markAppendBatch(System.nanoTime() - start);
      }
    }
  }

  static class ContinuousRun extends ClientRunnable {
    ContinuousRun(CountDownLatch latch, RpcClient client, ClientMetrics metrics, TaskConfiguration configuration) {
      super(latch, client, metrics, configuration);
    }

    @Override
    protected void doEach() {
      appendBatch(configuration().getBatchSize());
    }
  }
}
