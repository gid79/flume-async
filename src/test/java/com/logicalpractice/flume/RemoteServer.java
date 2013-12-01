package com.logicalpractice.flume;

import com.logicalpractice.flume.api.RpcTestUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import org.apache.avro.AvroRemoteException;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RemoteServer {

  public static void main(String[] args) {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("RemoteServer");

    Handler h = new Handler(100, TimeUnit.MILLISECONDS, new NormalDistribution(100, 10));
    RpcTestUtils.startServer(h, 2001);
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
