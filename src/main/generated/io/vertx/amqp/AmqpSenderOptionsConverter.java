package io.vertx.amqp;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Converter and mapper for {@link io.vertx.amqp.AmqpSenderOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqp.AmqpSenderOptions} original class using Vert.x codegen.
 */
public class AmqpSenderOptionsConverter {

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpSenderOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "linkName":
          if (member.getValue() instanceof String) {
            obj.setLinkName((String)member.getValue());
          }
          break;
        case "dynamic":
          if (member.getValue() instanceof Boolean) {
            obj.setDynamic((Boolean)member.getValue());
          }
          break;
        case "autoDrained":
          if (member.getValue() instanceof Boolean) {
            obj.setAutoDrained((Boolean)member.getValue());
          }
          break;
        case "capabilities":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<java.lang.String> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                list.add((String)item);
            });
            obj.setCapabilities(list);
          }
          break;
        case "capabilitys":
          if (member.getValue() instanceof JsonArray) {
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof String)
                obj.addCapability((String)item);
            });
          }
          break;
      }
    }
  }

   static void toJson(AmqpSenderOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(AmqpSenderOptions obj, java.util.Map<String, Object> json) {
    if (obj.getLinkName() != null) {
      json.put("linkName", obj.getLinkName());
    }
    json.put("dynamic", obj.isDynamic());
    json.put("autoDrained", obj.isAutoDrained());
    if (obj.getCapabilities() != null) {
      JsonArray array = new JsonArray();
      obj.getCapabilities().forEach(item -> array.add(item));
      json.put("capabilities", array);
    }
  }
}
