package com.logicalpractice.flume.api;

import java.util.concurrent.Delayed;

/**
 *
 */
interface Timeout extends Delayed,Comparable<Delayed>, Runnable {

  boolean isCancelled();
}
