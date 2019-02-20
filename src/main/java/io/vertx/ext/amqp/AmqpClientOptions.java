package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;
import io.vertx.proton.ProtonClientOptions;

import java.util.Set;
import java.util.UUID;

/**
 * Configures the AMQP Client.
 */
@DataObject(generateConverter = true, inheritConverter = true)
public class AmqpClientOptions extends ProtonClientOptions {

  // TODO Capabilities and properties

  private String host = getHostFromSysOrEnv();
  private int port = getPortFromSysOrEnv();

  private String username;
  private String password;
  private boolean replyEnabled = true;
  private long replyTimeout = 30_000;

  private String containerId = UUID.randomUUID().toString();
  private int maxBufferedMessages;

  public AmqpClientOptions() {
    super();
  }

  public AmqpClientOptions(JsonObject json) {
    super(json);
    AmqpClientOptionsConverter.fromJson(json, this);
  }

  public AmqpClientOptions(AmqpClientOptions other) {
    super(other);
    this.host = other.host;
    this.password = other.password;
    this.username = other.username;
    this.port = other.port;
    this.containerId = other.containerId;
    this.replyEnabled = other.replyEnabled;
    this.replyTimeout = other.replyTimeout;
    this.maxBufferedMessages = other.maxBufferedMessages;
  }

  public JsonObject toJson() {
    JsonObject json = super.toJson();
    AmqpClientOptionsConverter.toJson(this, json);
    return json;
  }


  /**
   * @return the host.
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the host.
   *
   * @param host the host, must not be {@code null} when the client attempt to connect.
   * @return the current {@link AmqpClientOptions}
   */
  public AmqpClientOptions setHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * @return the port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the port.
   *
   * @param port the port, defaults to 5672.
   * @return the current {@link AmqpClientOptions}
   */
  public AmqpClientOptions setPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * @return the username
   */
  public String getUsername() {
    return username;
  }

  /**
   * Sets the username.
   * @param username the username
   * @return the current {@link AmqpClientOptions}
   */
  public AmqpClientOptions setUsername(String username) {
    this.username = username;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public AmqpClientOptions getPassword(String pwd) {
    this.password = pwd;
    return this;
  }

  public String getContainerId() {
    return containerId;
  }

  public AmqpClientOptions setContainerId(String containerId) {
    this.containerId = containerId;
    return this;
  }

  @Override
  public AmqpClientOptions addEnabledSaslMechanism(String saslMechanism) {
    super.addEnabledSaslMechanism(saslMechanism);
    return this;
  }

  @Override
  public AmqpClientOptions setSendBufferSize(int sendBufferSize) {
    super.setSendBufferSize(sendBufferSize);
    return this;
  }

  @Override
  public AmqpClientOptions setReceiveBufferSize(int receiveBufferSize) {
    super.setReceiveBufferSize(receiveBufferSize);
    return this;
  }

  @Override
  public AmqpClientOptions setReuseAddress(boolean reuseAddress) {
    super.setReuseAddress(reuseAddress);
    return this;
  }

  @Override
  public AmqpClientOptions setTrafficClass(int trafficClass) {
    super.setTrafficClass(trafficClass);
    return this;
  }

  @Override
  public AmqpClientOptions setTcpNoDelay(boolean tcpNoDelay) {
    super.setTcpNoDelay(tcpNoDelay);
    return this;
  }

  @Override
  public AmqpClientOptions setTcpKeepAlive(boolean tcpKeepAlive) {
    super.setTcpKeepAlive(tcpKeepAlive);
    return this;
  }

  @Override
  public AmqpClientOptions setSoLinger(int soLinger) {
    super.setSoLinger(soLinger);
    return this;
  }

  @Override
  public AmqpClientOptions setUsePooledBuffers(boolean usePooledBuffers) {
    super.setUsePooledBuffers(usePooledBuffers);
    return this;
  }

  @Override
  public AmqpClientOptions setIdleTimeout(int idleTimeout) {
    super.setIdleTimeout(idleTimeout);
    return this;
  }

  @Override
  public AmqpClientOptions setSsl(boolean ssl) {
    super.setSsl(ssl);
    return this;
  }

  @Override
  public AmqpClientOptions setKeyStoreOptions(JksOptions options) {
    super.setKeyStoreOptions(options);
    return this;
  }

  @Override
  public AmqpClientOptions setPfxKeyCertOptions(PfxOptions options) {
    super.setPfxKeyCertOptions(options);
    return this;
  }

  @Override
  public AmqpClientOptions setPemKeyCertOptions(PemKeyCertOptions options) {
    super.setPemKeyCertOptions(options);
    return this;
  }

  @Override
  public AmqpClientOptions setTrustStoreOptions(JksOptions options) {
    super.setTrustStoreOptions(options);
    return this;
  }

  @Override
  public AmqpClientOptions setPemTrustOptions(PemTrustOptions options) {
    super.setPemTrustOptions(options);
    return this;
  }

  @Override
  public AmqpClientOptions setPfxTrustOptions(PfxOptions options) {
    super.setPfxTrustOptions(options);
    return this;
  }

  @Override
  public AmqpClientOptions addEnabledCipherSuite(String suite) {
    super.addEnabledCipherSuite(suite);
    return this;
  }

  @Override
  public AmqpClientOptions addCrlPath(String crlPath) {
    super.addCrlPath(crlPath);
    return this;
  }

  @Override
  public AmqpClientOptions addCrlValue(Buffer crlValue) {
    super.addCrlValue(crlValue);
    return this;
  }

  @Override
  public AmqpClientOptions setTrustAll(boolean trustAll) {
    super.setTrustAll(trustAll);
    return this;
  }

  @Override
  public AmqpClientOptions setConnectTimeout(int connectTimeout) {
    super.setConnectTimeout(connectTimeout);
    return this;
  }

  @Override
  public AmqpClientOptions setReconnectAttempts(int attempts) {
    super.setReconnectAttempts(attempts);
    return this;
  }

  @Override
  public AmqpClientOptions setReconnectInterval(long interval) {
    super.setReconnectInterval(interval);
    return this;
  }

  @Override
  public AmqpClientOptions addEnabledSecureTransportProtocol(String protocol) {
    super.addEnabledSecureTransportProtocol(protocol);
    return this;
  }

  @Override
  public AmqpClientOptions setHostnameVerificationAlgorithm(String hostnameVerificationAlgorithm) {
    super.setHostnameVerificationAlgorithm(hostnameVerificationAlgorithm);
    return this;
  }

  @Override
  public AmqpClientOptions setJdkSslEngineOptions(JdkSSLEngineOptions sslEngineOptions) {
    super.setJdkSslEngineOptions(sslEngineOptions);
    return this;
  }

  @Override
  public AmqpClientOptions setOpenSslEngineOptions(OpenSSLEngineOptions sslEngineOptions) {
    super.setOpenSslEngineOptions(sslEngineOptions);
    return this;
  }

  @Override
  public AmqpClientOptions setSslEngineOptions(SSLEngineOptions sslEngineOptions) {
    super.setSslEngineOptions(sslEngineOptions);
    return this;
  }

  @Override
  public AmqpClientOptions setLocalAddress(String localAddress) {
    super.setLocalAddress(localAddress);
    return this;
  }

  @Override
  public AmqpClientOptions setReusePort(boolean reusePort) {
    super.setReusePort(reusePort);
    return this;
  }

  @Override
  public AmqpClientOptions setTcpCork(boolean tcpCork) {
    super.setTcpCork(tcpCork);
    return this;
  }

  @Override
  public AmqpClientOptions setTcpFastOpen(boolean tcpFastOpen) {
    super.setTcpFastOpen(tcpFastOpen);
    return this;
  }

  @Override
  public AmqpClientOptions setTcpQuickAck(boolean tcpQuickAck) {
    super.setTcpQuickAck(tcpQuickAck);
    return this;
  }

  @Override
  public AmqpClientOptions removeEnabledSecureTransportProtocol(String protocol) {
    super.removeEnabledSecureTransportProtocol(protocol);
    return this;
  }

  @Override
  public AmqpClientOptions setEnabledSecureTransportProtocols(Set<String> enabledSecureTransportProtocols) {
    super.setEnabledSecureTransportProtocols(enabledSecureTransportProtocols);
    return this;
  }

  @Override
  public AmqpClientOptions setVirtualHost(String virtualHost) {
    super.setVirtualHost(virtualHost);
    return this;
  }

  @Override
  public AmqpClientOptions setSniServerName(String sniServerName) {
    super.setSniServerName(sniServerName);
    return this;
  }

  @Override
  public AmqpClientOptions setHeartbeat(int heartbeat) {
    super.setHeartbeat(heartbeat);
    return this;
  }

  @Override
  public AmqpClientOptions setMaxFrameSize(int maxFrameSize) {
    super.setMaxFrameSize(maxFrameSize);
    return this;
  }

  public AmqpClientOptions setPassword(String password) {
    this.password = password;
    return this;
  }

  private String getHostFromSysOrEnv() {
    String sys = System.getProperty("amqp-client-host");
    if (sys == null) {
      return System.getenv("AMQP_CLIENT_HOST");
    }
    return sys;
  }

  private int getPortFromSysOrEnv() {
    String sys = System.getProperty("amqp-client-port");
    if (sys == null) {
      String env = System.getenv("AMQP_CLIENT_PORT");
      if (env == null) {
        return 5672;
      } else {
        return Integer.parseInt(env);
      }
    }
    return Integer.parseInt(sys);
  }

  public boolean isReplyEnabled() {
    return replyEnabled;
  }

  public AmqpClientOptions setReplyEnabled(boolean replyEnabled) {
    this.replyEnabled = replyEnabled;
    return this;
  }

  public long getReplyTimeout() {
    return replyTimeout;
  }

  public AmqpClientOptions setReplyTimeout(long replyTimeout) {
    this.replyTimeout = replyTimeout;
    return this;
  }

  public AmqpClientOptions setMaxBufferedMessages(int maxBufferSize) {
    this.maxBufferedMessages = maxBufferSize;
    return this;
  }

  public int getMaxBufferedMessages() {
    return this.maxBufferedMessages;
  }
}
