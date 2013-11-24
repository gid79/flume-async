package com.logicalpractice.flume.api;

import org.apache.flume.api.RpcClientConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 *
 */
public class NettyConfiguration {
  private static Logger logger = LoggerFactory.getLogger(NettyConfiguration.class);

  public static Builder newBuilder() {
    return new Builder();
  }

  private final boolean enableCompression;
  private final int compressionLevel;
  private final boolean enableSsl;
  private final boolean trustAllCerts;
  private final String truststore;
  private final String truststorePassword;
  private final String truststoreType;

  /**
   * Use the Builder object in order to create this
   * @param enableCompression
   * @param compressionLevel
   * @param enableSsl
   * @param trustAllCerts
   * @param truststore
   * @param truststorePassword
   * @param truststoreType
   */
  private NettyConfiguration(
      boolean enableCompression,
      int compressionLevel,
      boolean enableSsl,
      boolean trustAllCerts,
      String truststore,
      String truststorePassword,
      String truststoreType) {
    this.enableCompression = enableCompression;
    this.compressionLevel = compressionLevel;
    this.enableSsl = enableSsl;
    this.trustAllCerts = trustAllCerts;
    this.truststore = truststore;
    this.truststorePassword = truststorePassword;
    this.truststoreType = truststoreType;
  }

  public boolean isEnableCompression() {
    return enableCompression;
  }

  public int getCompressionLevel() {
    return compressionLevel;
  }

  public boolean isEnableSsl() {
    return enableSsl;
  }

  public boolean isTrustAllCerts() {
    return trustAllCerts;
  }

  public String getTruststore() {
    return truststore;
  }

  public String getTruststorePassword() {
    return truststorePassword;
  }

  public String getTruststoreType() {
    return truststoreType;
  }

  @Override
  public String toString() {
    return "NettyConfiguration{" +
        "enableCompression=" + enableCompression +
        ", compressionLevel=" + compressionLevel +
        ", enableSsl=" + enableSsl +
        ", trustAllCerts=" + trustAllCerts +
        ", truststore='" + truststore + '\'' +
        ", truststorePassword='" + truststorePassword + '\'' +
        ", truststoreType='" + truststoreType + '\'' +
        '}';
  }

  public static Builder fromProperties(Properties properties) {
    Builder config = newBuilder();
    String enableCompressionStr = properties.getProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_TYPE);
    if (enableCompressionStr != null && enableCompressionStr.equalsIgnoreCase("deflate")) {
      config.enableCompression(true);
      String compressionLvlStr = properties.getProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_LEVEL);

      if (compressionLvlStr != null) {
        try {
          config.compressionLevel(Integer.parseInt(compressionLvlStr));
        } catch (NumberFormatException ex) {
          logger.error("Invalid compression level: " + compressionLvlStr);
        }
      }
    }

    config.enableSsl(Boolean.parseBoolean(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_SSL)));
    config.trustAllCerts(Boolean.parseBoolean(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_TRUST_ALL_CERTS)));
    config.truststore(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_TRUSTSTORE));
    config.truststorePassword(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_TRUSTSTORE_PASSWORD));
    config.truststoreType(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_TRUSTSTORE_TYPE, "JKS"));

    return config;
  }

  public static class Builder {
    private boolean enableCompression = false;
    private int compressionLevel = RpcClientConfigurationConstants.DEFAULT_COMPRESSION_LEVEL;
    private boolean enableSsl = false;
    private boolean trustAllCerts = false;
    private String truststore;
    private String truststorePassword;
    private String truststoreType = "JKS";

    public boolean isEnableCompression() {
      return enableCompression;
    }

    public void setEnableCompression(boolean enableCompression) {
      this.enableCompression = enableCompression;
    }

    public int getCompressionLevel() {
      return compressionLevel;
    }

    public void setCompressionLevel(int compressionLevel) {
      this.compressionLevel = compressionLevel;
    }

    public boolean isEnableSsl() {
      return enableSsl;
    }

    public void setEnableSsl(boolean enableSsl) {
      this.enableSsl = enableSsl;
    }

    public boolean isTrustAllCerts() {
      return trustAllCerts;
    }

    public void setTrustAllCerts(boolean trustAllCerts) {
      this.trustAllCerts = trustAllCerts;
    }

    public String getTruststore() {
      return truststore;
    }

    public void setTruststore(String truststore) {
      this.truststore = truststore;
    }

    public String getTruststorePassword() {
      return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
      this.truststorePassword = truststorePassword;
    }

    public String getTruststoreType() {
      return truststoreType;
    }

    public void setTruststoreType(String truststoreType) {
      this.truststoreType = truststoreType;
    }


    // fluent style methods
    public Builder enableCompression(boolean enableCompression) {
      setEnableCompression(enableCompression);
      return this;
    }


    public Builder compressionLevel(int compressionLevel) {
      setCompressionLevel(compressionLevel);
      return this;
    }


    public Builder enableSsl(boolean enableSsl) {
      setEnableSsl(enableSsl);
      return this;
    }

    public Builder trustAllCerts(boolean trustAllCerts) {
      setTrustAllCerts(trustAllCerts);
      return this;

    }

    public Builder truststore(String truststore) {
      setTruststore(truststore);
      return this;
    }

    public Builder truststorePassword(String truststorePassword) {
      setTruststorePassword(truststorePassword);
      return this;
    }

    public Builder truststoreType(String truststoreType) {
      setTruststoreType(truststoreType);
      return this;
    }

    public NettyConfiguration build() {
      return new NettyConfiguration(
          enableCompression,
          compressionLevel,
          enableSsl,
          trustAllCerts,
          truststore,
          truststorePassword,
          truststoreType
      );
    }
  }
}
