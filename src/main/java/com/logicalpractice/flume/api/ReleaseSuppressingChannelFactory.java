package com.logicalpractice.flume.api;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;

/**
 * Required in order to share the ChannelFactory between clients.
 *
 * The Avro NettyTransceiver calls 'releaseExternalResources' on being closed.
 */
class ReleaseSuppressingChannelFactory implements ChannelFactory {
  
  private final ChannelFactory delegate;

  public ReleaseSuppressingChannelFactory(ChannelFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public Channel newChannel(ChannelPipeline pipeline) {
    return delegate.newChannel(pipeline);
  }

  @Override
  public void releaseExternalResources() {
    // not 
  }
}
