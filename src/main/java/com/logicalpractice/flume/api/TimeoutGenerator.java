package com.logicalpractice.flume.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 *
 */
class TimeoutGenerator {
  private static Logger logger = LoggerFactory.getLogger(TimeoutGenerator.class);

  //todo replace delay queue with a LinkedHashSet, should provide O(1) insert and remove
  private DelayQueue<Timeout> delayQueue = new DelayQueue<Timeout>();

  private Semaphore threadRequired = new Semaphore(1);

  private volatile int threadId = 1;

  public void registerTimeout( Timeout timeout ) {
    delayQueue.add(timeout);

    if( threadRequired.tryAcquire() ) {
      Thread t = new Thread(new TimeoutWorker());
      t.setDaemon(true);
      t.setName("Timeout Generator " + threadId );
      threadId += 1;
      t.start();
    }
  }

  public void cancelTimeout( Timeout timeout ) {
    if( !timeout.isCancelled() ) {
      delayQueue.remove(timeout);
    }
  }

  public class TimeoutWorker implements Runnable {

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
      try {
        for(;;) {
          Timeout timeout = delayQueue.take();
          if( !timeout.isCancelled() ) {
            try {
              timeout.run();
            } catch (Throwable t) {
              logger.info("Unexpected exception caught processing a timeout, will ignore", t);
            }
          }
        }
      } catch (InterruptedException e) {
        logger.info("TimeoutWorker has been interrupted, will finish");
      } finally {
        threadRequired.release();
      }
    }
  }
}
