package io.vertx.ext.amqp;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter for {@link io.vertx.ext.amqp.AmqpLinkOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.ext.amqp.AmqpLinkOptions} original class using Vert.x codegen.
 */
public class AmqpLinkOptionsConverter {

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpLinkOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "autoDrained":
          if (member.getValue() instanceof Boolean) {
            obj.setAutoDrained((Boolean)member.getValue());
          }
          break;
        case "autoSettle":
          if (member.getValue() instanceof Boolean) {
            obj.setAutoSettle((Boolean)member.getValue());
          }
          break;
        case "dynamicAddress":
          if (member.getValue() instanceof Boolean) {
            obj.setDynamicAddress((Boolean)member.getValue());
          }
          break;
        case "name":
          if (member.getValue() instanceof String) {
            obj.setName((String)member.getValue());
          }
          break;
      }
    }
  }

  public static void toJson(AmqpLinkOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(AmqpLinkOptions obj, java.util.Map<String, Object> json) {
    json.put("autoDrained", obj.isAutoDrained());
    json.put("autoSettle", obj.isAutoSettle());
    json.put("dynamicAddress", obj.isDynamicAddress());
    if (obj.getName() != null) {
      json.put("name", obj.getName());
    }
  }
}
