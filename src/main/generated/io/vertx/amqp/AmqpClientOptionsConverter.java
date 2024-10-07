package io.vertx.amqp;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.amqp.AmqpClientOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqp.AmqpClientOptions} original class using Vert.x codegen.
 */
public class AmqpClientOptionsConverter {

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpClientOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
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

   static void toJson(AmqpClientOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(AmqpClientOptions obj, java.util.Map<String, Object> json) {
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
