package io.vertx.amqp;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

/**
 * Converter and mapper for {@link io.vertx.amqp.TargetOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqp.TargetOptions} original class using Vert.x codegen.
 */
public class TargetOptionsConverter {

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, TargetOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "address":
          if (member.getValue() instanceof String) {
            obj.setAddress((String)member.getValue());
          }
          break;
        case "durability":
          if (member.getValue() instanceof String) {
            obj.setDurability((String)member.getValue());
          }
          break;
        case "expiryPolicy":
          if (member.getValue() instanceof String) {
            obj.setExpiryPolicy((String)member.getValue());
          }
          break;
        case "timeout":
          if (member.getValue() instanceof Number) {
            obj.setTimeout(((Number)member.getValue()).intValue());
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
      }
    }
  }

   static void toJson(TargetOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(TargetOptions obj, java.util.Map<String, Object> json) {
    if (obj.getAddress() != null) {
      json.put("address", obj.getAddress());
    }
    if (obj.getDurability() != null) {
      json.put("durability", obj.getDurability());
    }
    if (obj.getExpiryPolicy() != null) {
      json.put("expiryPolicy", obj.getExpiryPolicy());
    }
    json.put("timeout", obj.getTimeout());
    if (obj.getCapabilities() != null) {
      JsonArray array = new JsonArray();
      obj.getCapabilities().forEach(item -> array.add(item));
      json.put("capabilities", array);
    }
  }
}
