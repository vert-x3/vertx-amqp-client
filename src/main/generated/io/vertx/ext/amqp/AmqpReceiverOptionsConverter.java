package io.vertx.ext.amqp;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter for {@link io.vertx.ext.amqp.AmqpReceiverOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.ext.amqp.AmqpReceiverOptions} original class using Vert.x codegen.
 */
public class AmqpReceiverOptionsConverter {

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpReceiverOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "dynamic":
          if (member.getValue() instanceof Boolean) {
            obj.setDynamic((Boolean)member.getValue());
          }
          break;
        case "linkName":
          if (member.getValue() instanceof String) {
            obj.setLinkName((String)member.getValue());
          }
          break;
        case "qos":
          if (member.getValue() instanceof String) {
            obj.setQos((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(AmqpReceiverOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(AmqpReceiverOptions obj, java.util.Map<String, Object> json) {
    json.put("dynamic", obj.isDynamic());
    if (obj.getLinkName() != null) {
      json.put("linkName", obj.getLinkName());
    }
    if (obj.getQos() != null) {
      json.put("qos", obj.getQos());
    }
  }
}
