package com.logicalpractice.flume.api;

import org.jboss.netty.channel.ChannelFactory;

/**
 *
 */
public interface SharableChannelFactory extends ChannelFactory {

    /**
     * @return a proxy for this ChannelFactory that suppresses the 'releaseExternalResources' method.
     */
    SharableChannelFactory releaseSuppressing();
}
