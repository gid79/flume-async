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

  private volatile Thread worker ;
  private volatile int threadId = 1;

  public void registerTimeout( Timeout timeout ) {
    delayQueue.add(timeout);

    if( threadRequired.tryAcquire() ) {
      Thread t = new Thread(new TimeoutWorker());
      t.setDaemon(true);
      t.setName("Timeout Generator " + threadId );
      threadId += 1;
      t.start();
      worker = t;
    }
  }

  public void cancelTimeout( Timeout timeout ) {
    if( !timeout.isCancelled() ) {
      delayQueue.remove(timeout);
    }
  }

  public void shutdown() {
    Thread t = worker;
    if( t != null && t.isAlive() ) {
      t.interrupt();
      try {
        t.join(300);
      } catch (InterruptedException e) {
        logger.info("Interrupted while waiting for worker to finish");
      }
      if( t.isAlive() ) {
        logger.info("Worker thread is still alive after being asked to finish, will ignore");
      }
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
        worker = null;
        threadRequired.release();
      }
    }
  }
}
