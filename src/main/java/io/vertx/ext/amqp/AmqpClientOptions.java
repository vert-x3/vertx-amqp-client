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
 * You can also configure the underlying Proton instance. Refer to {@link ProtonClientOptions} for details.
 */
@DataObject(generateConverter = true, inheritConverter = true)
public class AmqpClientOptions extends ProtonClientOptions {

  // TODO Capabilities and properties

  private String host = getHostFromSysOrEnv();
  private int port = getPortFromSysOrEnv();

  private String username = getUsernameFromSysOrEnv();
  private String password = getPasswordFromSysOrEnv();
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
   * @param host the host, must not be {@code null} when the client attempt to connect. Defaults to system variable
   *             {@code amqp-client-host} and to {@code AMQP_CLIENT_HOST} environment variable
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
   * @param port the port, defaults to system variable {@code amqp-client-port} and to {@code AMQP_CLIENT_PORT}
   *             environment variable and if neither is set {@code 5672}.
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
   *
   * @param username the username, defaults to system variable {@code amqp-client-username} and
   *                 to {@code AMQP_CLIENT_USERNAME} environment variable.
   * @return the current {@link AmqpClientOptions}
   */
  public AmqpClientOptions setUsername(String username) {
    this.username = username;
    return this;
  }

  /**
   * @return the password
   */
  public String getPassword() {
    return password;
  }

  /**
   * Sets the password.
   *
   * @param pwd the password, defaults to system variable {@code amqp-client-password} and to
   *            {@code AMQP_CLIENT_PASSWORD} environment variable.
   * @return the current {@link AmqpClientOptions}
   */
  public AmqpClientOptions setPassword(String pwd) {
    this.password = pwd;
    return this;
  }

  /**
   * @return the container id.
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * Sets the container id.
   *
   * @param containerId the container id
   * @return the current {@link AmqpClientOptions}
   */
  public AmqpClientOptions setContainerId(String containerId) {
    this.containerId = containerId;
    return this;
  }

  /**
   * @see ProtonClientOptions#addEnabledSaslMechanism(String)
   */
  @Override
  public AmqpClientOptions addEnabledSaslMechanism(String saslMechanism) {
    super.addEnabledSaslMechanism(saslMechanism);
    return this;
  }

  /**
   * @see ProtonClientOptions#setSendBufferSize(int)
   */
  @Override
  public AmqpClientOptions setSendBufferSize(int sendBufferSize) {
    super.setSendBufferSize(sendBufferSize);
    return this;
  }

  /**
   * @see ProtonClientOptions#setReceiveBufferSize(int)
   */
  @Override
  public AmqpClientOptions setReceiveBufferSize(int receiveBufferSize) {
    super.setReceiveBufferSize(receiveBufferSize);
    return this;
  }

  /**
   * @see ProtonClientOptions#setReuseAddress(boolean)
   */
  @Override
  public AmqpClientOptions setReuseAddress(boolean reuseAddress) {
    super.setReuseAddress(reuseAddress);
    return this;
  }

  /**
   * @see ProtonClientOptions#setTrafficClass(int)
   */
  @Override
  public AmqpClientOptions setTrafficClass(int trafficClass) {
    super.setTrafficClass(trafficClass);
    return this;
  }

  /**
   * @see ProtonClientOptions#setTcpNoDelay(boolean)
   */
  @Override
  public AmqpClientOptions setTcpNoDelay(boolean tcpNoDelay) {
    super.setTcpNoDelay(tcpNoDelay);
    return this;
  }

  /**
   * @see ProtonClientOptions#setTcpKeepAlive(boolean)
   */
  @Override
  public AmqpClientOptions setTcpKeepAlive(boolean tcpKeepAlive) {
    super.setTcpKeepAlive(tcpKeepAlive);
    return this;
  }

  /**
   * @see ProtonClientOptions#setSoLinger(int)
   */
  @Override
  public AmqpClientOptions setSoLinger(int soLinger) {
    super.setSoLinger(soLinger);
    return this;
  }

  /**
   * @see ProtonClientOptions#setUsePooledBuffers(boolean)
   */
  @Override
  public AmqpClientOptions setUsePooledBuffers(boolean usePooledBuffers) {
    super.setUsePooledBuffers(usePooledBuffers);
    return this;
  }

  /**
   * @see ProtonClientOptions#setIdleTimeout(int)
   */
  @Override
  public AmqpClientOptions setIdleTimeout(int idleTimeout) {
    super.setIdleTimeout(idleTimeout);
    return this;
  }

  /**
   * @see ProtonClientOptions#setSsl(boolean)
   */
  @Override
  public AmqpClientOptions setSsl(boolean ssl) {
    super.setSsl(ssl);
    return this;
  }

  /**
   * @see ProtonClientOptions#setKeyStoreOptions(JksOptions)
   */
  @Override
  public AmqpClientOptions setKeyStoreOptions(JksOptions options) {
    super.setKeyStoreOptions(options);
    return this;
  }

  /**
   * @see ProtonClientOptions#setPfxKeyCertOptions(PfxOptions)
   */
  @Override
  public AmqpClientOptions setPfxKeyCertOptions(PfxOptions options) {
    super.setPfxKeyCertOptions(options);
    return this;
  }

  /**
   * @see ProtonClientOptions#setPemKeyCertOptions(PemKeyCertOptions)
   */
  @Override
  public AmqpClientOptions setPemKeyCertOptions(PemKeyCertOptions options) {
    super.setPemKeyCertOptions(options);
    return this;
  }

  /**
   * @see ProtonClientOptions#setTrustStoreOptions(JksOptions)
   */
  @Override
  public AmqpClientOptions setTrustStoreOptions(JksOptions options) {
    super.setTrustStoreOptions(options);
    return this;
  }

  /**
   * @see ProtonClientOptions#setPemTrustOptions(PemTrustOptions)
   */
  @Override
  public AmqpClientOptions setPemTrustOptions(PemTrustOptions options) {
    super.setPemTrustOptions(options);
    return this;
  }

  /**
   * @see ProtonClientOptions#setPfxTrustOptions(PfxOptions)
   */
  @Override
  public AmqpClientOptions setPfxTrustOptions(PfxOptions options) {
    super.setPfxTrustOptions(options);
    return this;
  }

  /**
   * @see ProtonClientOptions#addEnabledCipherSuite(String)
   */
  @Override
  public AmqpClientOptions addEnabledCipherSuite(String suite) {
    super.addEnabledCipherSuite(suite);
    return this;
  }

  /**
   * @see ProtonClientOptions#addCrlPath(String)
   */
  @Override
  public AmqpClientOptions addCrlPath(String crlPath) {
    super.addCrlPath(crlPath);
    return this;
  }

  /**
   * @see ProtonClientOptions#addCrlValue(Buffer)
   */
  @Override
  public AmqpClientOptions addCrlValue(Buffer crlValue) {
    super.addCrlValue(crlValue);
    return this;
  }

  /**
   * @see ProtonClientOptions#setTrustAll(boolean)
   */
  @Override
  public AmqpClientOptions setTrustAll(boolean trustAll) {
    super.setTrustAll(trustAll);
    return this;
  }

  /**
   * @see ProtonClientOptions#setConnectTimeout(int)
   */
  @Override
  public AmqpClientOptions setConnectTimeout(int connectTimeout) {
    super.setConnectTimeout(connectTimeout);
    return this;
  }

  /**
   * @see ProtonClientOptions#setReconnectAttempts(int)
   */
  @Override
  public AmqpClientOptions setReconnectAttempts(int attempts) {
    super.setReconnectAttempts(attempts);
    return this;
  }

  /**
   * @see ProtonClientOptions#setReconnectInterval(long)
   */
  @Override
  public AmqpClientOptions setReconnectInterval(long interval) {
    super.setReconnectInterval(interval);
    return this;
  }

  /**
   * @see ProtonClientOptions#addEnabledSecureTransportProtocol(String)
   */
  @Override
  public AmqpClientOptions addEnabledSecureTransportProtocol(String protocol) {
    super.addEnabledSecureTransportProtocol(protocol);
    return this;
  }

  /**
   * @see ProtonClientOptions#setHostnameVerificationAlgorithm(String)
   */
  @Override
  public AmqpClientOptions setHostnameVerificationAlgorithm(String hostnameVerificationAlgorithm) {
    super.setHostnameVerificationAlgorithm(hostnameVerificationAlgorithm);
    return this;
  }

  /**
   * @see ProtonClientOptions#setJdkSslEngineOptions(JdkSSLEngineOptions)
   */
  @Override
  public AmqpClientOptions setJdkSslEngineOptions(JdkSSLEngineOptions sslEngineOptions) {
    super.setJdkSslEngineOptions(sslEngineOptions);
    return this;
  }

  /**
   * @see ProtonClientOptions#setOpenSslEngineOptions(OpenSSLEngineOptions)
   */
  @Override
  public AmqpClientOptions setOpenSslEngineOptions(OpenSSLEngineOptions sslEngineOptions) {
    super.setOpenSslEngineOptions(sslEngineOptions);
    return this;
  }

  /**
   * @see ProtonClientOptions#setSslEngineOptions(SSLEngineOptions)
   */
  @Override
  public AmqpClientOptions setSslEngineOptions(SSLEngineOptions sslEngineOptions) {
    super.setSslEngineOptions(sslEngineOptions);
    return this;
  }

  /**
   * @see ProtonClientOptions#setLocalAddress(String)
   */
  @Override
  public AmqpClientOptions setLocalAddress(String localAddress) {
    super.setLocalAddress(localAddress);
    return this;
  }

  /**
   * @see ProtonClientOptions#setReusePort(boolean)
   */
  @Override
  public AmqpClientOptions setReusePort(boolean reusePort) {
    super.setReusePort(reusePort);
    return this;
  }

  /**
   * @see ProtonClientOptions#setTcpCork(boolean)
   */
  @Override
  public AmqpClientOptions setTcpCork(boolean tcpCork) {
    super.setTcpCork(tcpCork);
    return this;
  }

  /**
   * @see ProtonClientOptions#setTcpFastOpen(boolean)
   */
  @Override
  public AmqpClientOptions setTcpFastOpen(boolean tcpFastOpen) {
    super.setTcpFastOpen(tcpFastOpen);
    return this;
  }

  /**
   * @see ProtonClientOptions#setTcpQuickAck(boolean)
   */
  @Override
  public AmqpClientOptions setTcpQuickAck(boolean tcpQuickAck) {
    super.setTcpQuickAck(tcpQuickAck);
    return this;
  }

  /**
   * @see ProtonClientOptions#removeEnabledSecureTransportProtocol(String)
   */
  @Override
  public AmqpClientOptions removeEnabledSecureTransportProtocol(String protocol) {
    super.removeEnabledSecureTransportProtocol(protocol);
    return this;
  }

  /**
   * @see ProtonClientOptions#setEnabledSecureTransportProtocols(Set)
   */
  @Override
  public AmqpClientOptions setEnabledSecureTransportProtocols(Set<String> enabledSecureTransportProtocols) {
    super.setEnabledSecureTransportProtocols(enabledSecureTransportProtocols);
    return this;
  }

  /**
   * @see ProtonClientOptions#setVirtualHost(String)
   */
  @Override
  public AmqpClientOptions setVirtualHost(String virtualHost) {
    super.setVirtualHost(virtualHost);
    return this;
  }

  /**
   * @see ProtonClientOptions#setSniServerName(String)
   */
  @Override
  public AmqpClientOptions setSniServerName(String sniServerName) {
    super.setSniServerName(sniServerName);
    return this;
  }

  /**
   * @see ProtonClientOptions#setHeartbeat(int)
   */
  @Override
  public AmqpClientOptions setHeartbeat(int heartbeat) {
    super.setHeartbeat(heartbeat);
    return this;
  }

  /**
   * @see ProtonClientOptions#setMaxFrameSize(int)
   */
  @Override
  public AmqpClientOptions setMaxFrameSize(int maxFrameSize) {
    super.setMaxFrameSize(maxFrameSize);
    return this;
  }

  private String getHostFromSysOrEnv() {
    String sys = System.getProperty("amqp-client-host");
    if (sys == null) {
      return System.getenv("AMQP_CLIENT_HOST");
    }
    return sys;
  }

  private String getUsernameFromSysOrEnv() {
    String sys = System.getProperty("amqp-client-username");
    if (sys == null) {
      return System.getenv("AMQP_CLIENT_USERNAME");
    }
    return sys;
  }

  private String getPasswordFromSysOrEnv() {
    String sys = System.getProperty("amqp-client-password");
    if (sys == null) {
      return System.getenv("AMQP_CLIENT_PASSWORD");
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

  /**
   * @return whether reply support is enabled.
   */
  public boolean isReplyEnabled() {
    return replyEnabled;
  }

  /**
   * Enables or disables reply support.
   *
   * @param replyEnabled whether or not reply support is enabled.
   * @return the current {@link AmqpClientOptions} instance.
   */
  public AmqpClientOptions setReplyEnabled(boolean replyEnabled) {
    this.replyEnabled = replyEnabled;
    return this;
  }

  /**
   * @return the reply timeout. 30s by default.
   */
  public long getReplyTimeout() {
    return replyTimeout;
  }

  /**
   * Sets the reply timeout.
   *
   * @param replyTimeout the reply timeout in ms.
   * @return the current {@link AmqpClientOptions} instance.
   */
  public AmqpClientOptions setReplyTimeout(long replyTimeout) {
    this.replyTimeout = replyTimeout;
    return this;
  }

  /**
   * Sets the max buffered messages. This message can be used to configure the initial credit of a receiver.
   *
   * @param maxBufferSize the max buffered size, must be positive. If not set, default credit is used.
   * @return the current {@link AmqpClientOptions} instance.
   */
  public AmqpClientOptions setMaxBufferedMessages(int maxBufferSize) {
    this.maxBufferedMessages = maxBufferSize;
    return this;
  }

  /**
   * @return the max buffered messages
   */
  public int getMaxBufferedMessages() {
    return this.maxBufferedMessages;
  }
}
