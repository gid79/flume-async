package com.logicalpractice.flume.api;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

/**
 *
 */
class SelfClosingSocketChannelFactory implements ChannelFactory {

  private final ChannelFactory delegate;
  private final ChannelGroup group = new DefaultChannelGroup("avro rpc channels");

  public SelfClosingSocketChannelFactory(ChannelFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public Channel newChannel(ChannelPipeline pipeline) {
    Channel channel = delegate.newChannel(pipeline);
    group.add(channel); // note that the DefaultChannelGroup automagically removes closed channels
                        // thus we don't have to worry about removing or leaking channels
    return channel;
  }

  @Override
  public void releaseExternalResources() {
    group.close();
    delegate.releaseExternalResources();
  }
}
