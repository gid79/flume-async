package com.logicalpractice.flume.api;

import org.apache.avro.ipc.NettyTransceiver;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides a static factory method to create Netty NioClientSocketChannelFactory instances.
 *
 * This will allow Netty based clients to share a single ClientSocketChannelFactory and which should in
 * theory save on the number of threads being created in the VM.
 *
 * Factory for Factory's ... oh my it's spring all over again.
 */
public class NettySocketChannels {

  public static SharableChannelFactory newSocketChannelFactory() {
    return newSocketChannelFactory(NettyConfiguration.newBuilder().build());
  }

  public static SharableChannelFactory newSocketChannelFactory(NettyConfiguration configuration) {
    if( configuration == null ) {
      throw new IllegalArgumentException("'configuration' is required");
    }

    NioClientSocketChannelFactory socketChannelFactory ;

    ExecutorService bossExecutor = Executors.newCachedThreadPool(new PrefixedDaemonThreadFactory(
        "Avro " + NettyTransceiver.class.getSimpleName() + " Boss"));
    ExecutorService workerExecutor = Executors.newCachedThreadPool(new PrefixedDaemonThreadFactory(
        "Avro " + NettyTransceiver.class.getSimpleName() + " I/O Worker"));

    if (configuration.isEnableCompression() || configuration.isEnableSsl()) {
      socketChannelFactory = new SSLCompressionChannelFactory(
          bossExecutor,
          workerExecutor,
          configuration);
    } else {
      socketChannelFactory = new NioClientSocketChannelFactory(
          bossExecutor,
          workerExecutor);
    }
    return sharableChannelFactory(socketChannelFactory);
  }

  public static SharableChannelFactory sharableChannelFactory(final ChannelFactory socketChannelFactory) {
    return new SharableChannelFactory() {
      final ReleaseSuppressingChannelFactory releaseSuppressing = new ReleaseSuppressingChannelFactory(socketChannelFactory);
      @Override
      public SharableChannelFactory releaseSuppressing() {
        return releaseSuppressing;
      }

      @Override
      public Channel newChannel(ChannelPipeline pipeline) {
        return socketChannelFactory.newChannel(pipeline);
      }

      @Override
      public void releaseExternalResources() {
        socketChannelFactory.releaseExternalResources();
      }
    };
  }

    /**
   * Factory of SSL-enabled client channels
   * Copied from Avro's org.apache.flume.api.NettyAvroRpcClient
   */
  private static class SSLCompressionChannelFactory extends NioClientSocketChannelFactory {
    private static Logger logger = LoggerFactory.getLogger(SSLCompressionChannelFactory.class);

    private final NettyConfiguration config;

    public SSLCompressionChannelFactory(Executor bossExecutor, Executor workerExecutor,
        NettyConfiguration config) {
      super(bossExecutor, workerExecutor);
      this.config = config;
    }

    @Override
    public SocketChannel newChannel(ChannelPipeline pipeline) {
      TrustManager[] managers;
      try {
        if (config.isEnableCompression()) {
          ZlibEncoder encoder = new ZlibEncoder(config.getCompressionLevel());
          pipeline.addFirst("deflater", encoder);
          pipeline.addFirst("inflater", new ZlibDecoder());
        }
        if (config.isEnableSsl()) {
          if (config.isTrustAllCerts()) {
            logger.warn("No truststore configured, setting TrustManager to accept"
                + " all server certificates");
            managers = new TrustManager[] { new PermissiveTrustManager() };
          } else {
            KeyStore keystore = null;

            if (config.getTruststore() != null) {
              if (config.getTruststorePassword() == null) {
                throw new NullPointerException("truststore password is null");
              }
              InputStream truststoreStream = new FileInputStream(config.getTruststore());
              keystore = KeyStore.getInstance(config.getTruststoreType());
              keystore.load(truststoreStream, config.getTruststorePassword().toCharArray());
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            // null keystore is OK, with SunX509 it defaults to system CA Certs
            // see http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#X509TrustManager
            tmf.init(keystore);
            managers = tmf.getTrustManagers();
          }

          SSLContext sslContext = SSLContext.getInstance("TLS");
          sslContext.init(null, managers, null);
          SSLEngine sslEngine = sslContext.createSSLEngine();
          sslEngine.setUseClientMode(true);
          // addFirst() will make SSL handling the first stage of decoding
          // and the last stage of encoding this must be added after
          // adding compression handling above
          pipeline.addFirst("ssl", new SslHandler(sslEngine));
        }

        return super.newChannel(pipeline);
      } catch (Exception ex) {
        logger.error("Cannot create SSL channel", ex);
        throw new RuntimeException("Cannot create SSL channel", ex);
      }
    }
  }

  /**
   * Permissive trust manager accepting any certificate
   */
  private static class PermissiveTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }
}
