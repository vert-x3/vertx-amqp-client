/*
 * Copyright (c) 2018-2019 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *        The Eclipse Public License is available at
 *        http://www.eclipse.org/legal/epl-v10.html
 *
 *        The Apache License v2.0 is available at
 *        http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.amqp;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link io.vertx.amqp.AmqpClientOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqp.AmqpClientOptions} original class using Vert.x codegen.
 */
public class AmqpClientOptionsConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpClientOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "sendBufferSize":
          if (member.getValue() instanceof Number) {
            obj.setSendBufferSize(((Number)member.getValue()).intValue());
          }
          break;
        case "receiveBufferSize":
          if (member.getValue() instanceof Number) {
            obj.setReceiveBufferSize(((Number)member.getValue()).intValue());
          }
          break;
        case "reuseAddress":
          if (member.getValue() instanceof Boolean) {
            obj.setReuseAddress((Boolean)member.getValue());
          }
          break;
        case "trafficClass":
          if (member.getValue() instanceof Number) {
            obj.setTrafficClass(((Number)member.getValue()).intValue());
          }
          break;
        case "logActivity":
          if (member.getValue() instanceof Boolean) {
            obj.setLogActivity((Boolean)member.getValue());
          }
          break;
        case "activityLogDataFormat":
          if (member.getValue() instanceof String) {
            obj.setActivityLogDataFormat(io.netty.handler.logging.ByteBufFormat.valueOf((String)member.getValue()));
          }
          break;
        case "reusePort":
          if (member.getValue() instanceof Boolean) {
            obj.setReusePort((Boolean)member.getValue());
          }
          break;
        case "tcpNoDelay":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpNoDelay((Boolean)member.getValue());
          }
          break;
        case "tcpKeepAlive":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpKeepAlive((Boolean)member.getValue());
          }
          break;
        case "soLinger":
          if (member.getValue() instanceof Number) {
            obj.setSoLinger(((Number)member.getValue()).intValue());
          }
          break;
        case "idleTimeout":
          if (member.getValue() instanceof Number) {
            obj.setIdleTimeout(((Number)member.getValue()).intValue());
          }
          break;
        case "readIdleTimeout":
          if (member.getValue() instanceof Number) {
            obj.setReadIdleTimeout(((Number)member.getValue()).intValue());
          }
          break;
        case "writeIdleTimeout":
          if (member.getValue() instanceof Number) {
            obj.setWriteIdleTimeout(((Number)member.getValue()).intValue());
          }
          break;
        case "idleTimeoutUnit":
          if (member.getValue() instanceof String) {
            obj.setIdleTimeoutUnit(java.util.concurrent.TimeUnit.valueOf((String)member.getValue()));
          }
          break;
        case "ssl":
          if (member.getValue() instanceof Boolean) {
            obj.setSsl((Boolean)member.getValue());
          }
          break;
        case "enabledCipherSuites":
          if (member.getValue() instanceof JsonArray) {
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                obj.addEnabledCipherSuite((String)item);
            });
          }
          break;
        case "crlPaths":
          if (member.getValue() instanceof JsonArray) {
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                obj.addCrlPath((String)item);
            });
          }
          break;
        case "crlValues":
          if (member.getValue() instanceof JsonArray) {
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                obj.addCrlValue(io.vertx.core.buffer.Buffer.buffer(BASE64_DECODER.decode((String)item)));
            });
          }
          break;
        case "useAlpn":
          if (member.getValue() instanceof Boolean) {
            obj.setUseAlpn((Boolean)member.getValue());
          }
          break;
        case "openSslEngineOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setOpenSslEngineOptions(new io.vertx.core.net.OpenSSLEngineOptions((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "tcpFastOpen":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpFastOpen((Boolean)member.getValue());
          }
          break;
        case "tcpCork":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpCork((Boolean)member.getValue());
          }
          break;
        case "tcpQuickAck":
          if (member.getValue() instanceof Boolean) {
            obj.setTcpQuickAck((Boolean)member.getValue());
          }
          break;
        case "tcpUserTimeout":
          if (member.getValue() instanceof Number) {
            obj.setTcpUserTimeout(((Number)member.getValue()).intValue());
          }
          break;
        case "enabledSecureTransportProtocols":
          if (member.getValue() instanceof JsonArray) {
            java.util.LinkedHashSet<java.lang.String> list =  new java.util.LinkedHashSet<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                list.add((String)item);
            });
            obj.setEnabledSecureTransportProtocols(list);
          }
          break;
        case "sslHandshakeTimeout":
          if (member.getValue() instanceof Number) {
            obj.setSslHandshakeTimeout(((Number)member.getValue()).longValue());
          }
          break;
        case "sslHandshakeTimeoutUnit":
          if (member.getValue() instanceof String) {
            obj.setSslHandshakeTimeoutUnit(java.util.concurrent.TimeUnit.valueOf((String)member.getValue()));
          }
          break;
        case "trustAll":
          if (member.getValue() instanceof Boolean) {
            obj.setTrustAll((Boolean)member.getValue());
          }
          break;
        case "connectTimeout":
          if (member.getValue() instanceof Number) {
            obj.setConnectTimeout(((Number)member.getValue()).intValue());
          }
          break;
        case "metricsName":
          if (member.getValue() instanceof String) {
            obj.setMetricsName((String)member.getValue());
          }
          break;
        case "proxyOptions":
          if (member.getValue() instanceof JsonObject) {
            obj.setProxyOptions(new io.vertx.core.net.ProxyOptions((io.vertx.core.json.JsonObject)member.getValue()));
          }
          break;
        case "nonProxyHosts":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<java.lang.String> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                list.add((String)item);
            });
            obj.setNonProxyHosts(list);
          }
          break;
        case "localAddress":
          if (member.getValue() instanceof String) {
            obj.setLocalAddress((String)member.getValue());
          }
          break;
        case "reconnectAttempts":
          if (member.getValue() instanceof Number) {
            obj.setReconnectAttempts(((Number)member.getValue()).intValue());
          }
          break;
        case "hostnameVerificationAlgorithm":
          if (member.getValue() instanceof String) {
            obj.setHostnameVerificationAlgorithm((String)member.getValue());
          }
          break;
        case "applicationLayerProtocols":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<java.lang.String> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                list.add((String)item);
            });
            obj.setApplicationLayerProtocols(list);
          }
          break;
        case "reconnectInterval":
          if (member.getValue() instanceof Number) {
            obj.setReconnectInterval(((Number)member.getValue()).longValue());
          }
          break;
        case "registerWriteHandler":
          if (member.getValue() instanceof Boolean) {
            obj.setRegisterWriteHandler((Boolean)member.getValue());
          }
          break;
        case "enabledSaslMechanisms":
          if (member.getValue() instanceof JsonArray) {
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                obj.addEnabledSaslMechanism((String)item);
            });
          }
          break;
        case "virtualHost":
          if (member.getValue() instanceof String) {
            obj.setVirtualHost((String)member.getValue());
          }
          break;
        case "sniServerName":
          if (member.getValue() instanceof String) {
            obj.setSniServerName((String)member.getValue());
          }
          break;
        case "heartbeat":
          if (member.getValue() instanceof Number) {
            obj.setHeartbeat(((Number)member.getValue()).intValue());
          }
          break;
        case "maxFrameSize":
          if (member.getValue() instanceof Number) {
            obj.setMaxFrameSize(((Number)member.getValue()).intValue());
          }
          break;
        case "host":
          if (member.getValue() instanceof String) {
            obj.setHost((String)member.getValue());
          }
          break;
        case "port":
          if (member.getValue() instanceof Number) {
            obj.setPort(((Number)member.getValue()).intValue());
          }
          break;
        case "username":
          if (member.getValue() instanceof String) {
            obj.setUsername((String)member.getValue());
          }
          break;
        case "password":
          if (member.getValue() instanceof String) {
            obj.setPassword((String)member.getValue());
          }
          break;
        case "containerId":
          if (member.getValue() instanceof String) {
            obj.setContainerId((String)member.getValue());
          }
          break;
        case "connectionHostname":
          if (member.getValue() instanceof String) {
            obj.setConnectionHostname((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(AmqpClientOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(AmqpClientOptions obj, java.util.Map<String, Object> json) {
    json.put("sendBufferSize", obj.getSendBufferSize());
    json.put("receiveBufferSize", obj.getReceiveBufferSize());
    json.put("reuseAddress", obj.isReuseAddress());
    json.put("trafficClass", obj.getTrafficClass());
    json.put("logActivity", obj.getLogActivity());
    if (obj.getActivityLogDataFormat() != null) {
      json.put("activityLogDataFormat", obj.getActivityLogDataFormat().name());
    }
    json.put("reusePort", obj.isReusePort());
    json.put("tcpNoDelay", obj.isTcpNoDelay());
    json.put("tcpKeepAlive", obj.isTcpKeepAlive());
    json.put("soLinger", obj.getSoLinger());
    json.put("idleTimeout", obj.getIdleTimeout());
    json.put("readIdleTimeout", obj.getReadIdleTimeout());
    json.put("writeIdleTimeout", obj.getWriteIdleTimeout());
    if (obj.getIdleTimeoutUnit() != null) {
      json.put("idleTimeoutUnit", obj.getIdleTimeoutUnit().name());
    }
    json.put("ssl", obj.isSsl());
    if (obj.getEnabledCipherSuites() != null) {
      JsonArray array = new JsonArray();
      obj.getEnabledCipherSuites().forEach(item -> array.add(item));
      json.put("enabledCipherSuites", array);
    }
    if (obj.getCrlPaths() != null) {
      JsonArray array = new JsonArray();
      obj.getCrlPaths().forEach(item -> array.add(item));
      json.put("crlPaths", array);
    }
    if (obj.getCrlValues() != null) {
      JsonArray array = new JsonArray();
      obj.getCrlValues().forEach(item -> array.add(BASE64_ENCODER.encodeToString(item.getBytes())));
      json.put("crlValues", array);
    }
    json.put("useAlpn", obj.isUseAlpn());
    if (obj.getOpenSslEngineOptions() != null) {
      json.put("openSslEngineOptions", obj.getOpenSslEngineOptions().toJson());
    }
    json.put("tcpFastOpen", obj.isTcpFastOpen());
    json.put("tcpCork", obj.isTcpCork());
    json.put("tcpQuickAck", obj.isTcpQuickAck());
    json.put("tcpUserTimeout", obj.getTcpUserTimeout());
    if (obj.getEnabledSecureTransportProtocols() != null) {
      JsonArray array = new JsonArray();
      obj.getEnabledSecureTransportProtocols().forEach(item -> array.add(item));
      json.put("enabledSecureTransportProtocols", array);
    }
    json.put("sslHandshakeTimeout", obj.getSslHandshakeTimeout());
    if (obj.getSslHandshakeTimeoutUnit() != null) {
      json.put("sslHandshakeTimeoutUnit", obj.getSslHandshakeTimeoutUnit().name());
    }
    json.put("trustAll", obj.isTrustAll());
    json.put("connectTimeout", obj.getConnectTimeout());
    if (obj.getMetricsName() != null) {
      json.put("metricsName", obj.getMetricsName());
    }
    if (obj.getProxyOptions() != null) {
      json.put("proxyOptions", obj.getProxyOptions().toJson());
    }
    if (obj.getNonProxyHosts() != null) {
      JsonArray array = new JsonArray();
      obj.getNonProxyHosts().forEach(item -> array.add(item));
      json.put("nonProxyHosts", array);
    }
    if (obj.getLocalAddress() != null) {
      json.put("localAddress", obj.getLocalAddress());
    }
    json.put("reconnectAttempts", obj.getReconnectAttempts());
    if (obj.getHostnameVerificationAlgorithm() != null) {
      json.put("hostnameVerificationAlgorithm", obj.getHostnameVerificationAlgorithm());
    }
    if (obj.getApplicationLayerProtocols() != null) {
      JsonArray array = new JsonArray();
      obj.getApplicationLayerProtocols().forEach(item -> array.add(item));
      json.put("applicationLayerProtocols", array);
    }
    json.put("reconnectInterval", obj.getReconnectInterval());
    json.put("registerWriteHandler", obj.isRegisterWriteHandler());
    if (obj.getEnabledSaslMechanisms() != null) {
      JsonArray array = new JsonArray();
      obj.getEnabledSaslMechanisms().forEach(item -> array.add(item));
      json.put("enabledSaslMechanisms", array);
    }
    if (obj.getVirtualHost() != null) {
      json.put("virtualHost", obj.getVirtualHost());
    }
    if (obj.getSniServerName() != null) {
      json.put("sniServerName", obj.getSniServerName());
    }
    json.put("heartbeat", obj.getHeartbeat());
    json.put("maxFrameSize", obj.getMaxFrameSize());
    if (obj.getHost() != null) {
      json.put("host", obj.getHost());
    }
    json.put("port", obj.getPort());
    if (obj.getUsername() != null) {
      json.put("username", obj.getUsername());
    }
    if (obj.getPassword() != null) {
      json.put("password", obj.getPassword());
    }
    if (obj.getContainerId() != null) {
      json.put("containerId", obj.getContainerId());
    }
    if (obj.getConnectionHostname() != null) {
      json.put("connectionHostname", obj.getConnectionHostname());
    }
  }
}
