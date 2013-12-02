package com.logicalpractice.flume.performance;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.logicalpractice.flume.api.RpcTestUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.AvroRemoteException;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 *
 */
public class RemoteServer {

  public static void main(String[] args) {
    ArgumentParser parser = buildArgumentParser();

    Namespace ns = null;
    try {
        ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
        parser.handleError(e);
        System.exit(1);
    }
    configureLogging(ns);
    Handler h = new Handler(
        ns.getInt("delay"),
        TimeUnit.MILLISECONDS,
        null
    );
    RpcTestUtils.startServer(h, 2001);
  }

  private static ArgumentParser buildArgumentParser() {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("RemoteServer");

    parser.addArgument("-v","--verbose")
        .action(storeTrue())
        .setDefault(false)
    ;

    parser.addArgument("-d","--delay")
        .type(Integer.class)
        .setDefault(-1);

    return parser;
  }

  private static void configureLogging(Namespace ns) {
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    Logger root = lc.getLogger(Logger.ROOT_LOGGER_NAME);
    if( ns.getBoolean("verbose") ){
      root.setLevel(Level.DEBUG);
    } else {
      root.setLevel(Level.ERROR);
      lc.getLogger(RemoteServer.class).setLevel(Level.INFO);
    }
  }
  static class Handler implements AvroSourceProtocol {
    private final long delayNS ;
    private final RealDistribution distribution;

    Handler(long delay,TimeUnit units, RealDistribution distribution) {
      this.delayNS = units.toNanos(delay);
      this.distribution = distribution;
    }

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      applyDelay();
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) throws AvroRemoteException {
      applyDelay();
      return Status.OK;
    }

    private void applyDelay() {
      if( delayNS >= 0 ) {
        long thisDelay = Math.abs(delayNS + (long) distribution.sample());
        if( thisDelay != 0 ) {
          try {
            TimeUnit.NANOSECONDS.sleep(thisDelay);
          } catch (InterruptedException e) {
            // ignore and continue
          }
        }
      }
    }
  }
}
