package com.logicalpractice.flume.api;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread factor implementation modeled after the implementation of
 * NettyTransceiver.NettyTransceiverThreadFactory class which is
 * a private static class. The only difference between that and this
 * implementation is that this implementation marks all the threads daemon
 * which allows the termination of the VM when the non-daemon threads
 * are done.
 */
class PrefixedDaemonThreadFactory implements ThreadFactory {
  private final AtomicInteger threadId = new AtomicInteger(0);
  private final String prefix;

  /**
   * Creates a TransceiverThreadFactory that creates threads with the
   * specified name.
   * @param prefix the name prefix to use for all threads created by this
   * ThreadFactory.  A unique ID will be appended to this prefix to form the
   * final thread name.
   */
  public PrefixedDaemonThreadFactory(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread thread = new Thread(r);
    thread.setDaemon(true);
    thread.setName(prefix + " " + threadId.incrementAndGet());
    return thread;
  }
}
