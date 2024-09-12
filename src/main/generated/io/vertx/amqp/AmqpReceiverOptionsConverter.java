package io.vertx.amqp;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link io.vertx.amqp.AmqpReceiverOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqp.AmqpReceiverOptions} original class using Vert.x codegen.
 */
public class AmqpReceiverOptionsConverter {

  private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();
  private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpReceiverOptions obj) {
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
        case "qos":
          if (member.getValue() instanceof String) {
            obj.setQos((String)member.getValue());
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
        case "durable":
          if (member.getValue() instanceof Boolean) {
            obj.setDurable((Boolean)member.getValue());
          }
          break;
        case "maxBufferedMessages":
          if (member.getValue() instanceof Number) {
            obj.setMaxBufferedMessages(((Number)member.getValue()).intValue());
          }
          break;
        case "autoAcknowledgement":
          if (member.getValue() instanceof Boolean) {
            obj.setAutoAcknowledgement((Boolean)member.getValue());
          }
          break;
        case "selector":
          if (member.getValue() instanceof String) {
            obj.setSelector((String)member.getValue());
          }
          break;
        case "noLocal":
          if (member.getValue() instanceof Boolean) {
            obj.setNoLocal((Boolean)member.getValue());
          }
          break;
      }
    }
  }

   static void toJson(AmqpReceiverOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(AmqpReceiverOptions obj, java.util.Map<String, Object> json) {
    if (obj.getLinkName() != null) {
      json.put("linkName", obj.getLinkName());
    }
    json.put("dynamic", obj.isDynamic());
    if (obj.getQos() != null) {
      json.put("qos", obj.getQos());
    }
    if (obj.getCapabilities() != null) {
      JsonArray array = new JsonArray();
      obj.getCapabilities().forEach(item -> array.add(item));
      json.put("capabilities", array);
    }
    json.put("durable", obj.isDurable());
    json.put("maxBufferedMessages", obj.getMaxBufferedMessages());
    json.put("autoAcknowledgement", obj.isAutoAcknowledgement());
    if (obj.getSelector() != null) {
      json.put("selector", obj.getSelector());
    }
    json.put("noLocal", obj.isNoLocal());
  }
}
