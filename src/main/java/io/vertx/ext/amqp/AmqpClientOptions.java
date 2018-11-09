package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;
import io.vertx.proton.ProtonClientOptions;

import java.util.Set;

@DataObject(generateConverter = true, publicConverter = true, inheritConverter = true)
public class AmqpClientOptions extends ProtonClientOptions {

    // TODO Capabilities and properties
    // TODO default sender and receiver options

    private String host;
    private int port;
    private String username;
    private String pwd;

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
        this.pwd = other.pwd;
        this.username = other.username;
        this.port = other.port;
    }

    public JsonObject toJson() {
        JsonObject json = super.toJson();
        AmqpClientOptionsConverter.toJson(this, json);
        return json;
    }


    public String getHost() {
        return host;
    }

    public AmqpClientOptions setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public AmqpClientOptions setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public AmqpClientOptions setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return pwd;
    }

    public AmqpClientOptions getPassword(String pwd) {
        this.pwd = pwd;
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
    public AmqpClientOptions addCrlPath(String crlPath) throws NullPointerException {
        super.addCrlPath(crlPath);
        return this;
    }

    @Override
    public AmqpClientOptions addCrlValue(Buffer crlValue) throws NullPointerException {
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
}
