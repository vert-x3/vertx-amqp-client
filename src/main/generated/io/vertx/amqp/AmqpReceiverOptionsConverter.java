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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import io.vertx.core.spi.json.JsonCodec;

/**
 * Converter and Codec for {@link io.vertx.amqp.AmqpReceiverOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqp.AmqpReceiverOptions} original class using Vert.x codegen.
 */
public class AmqpReceiverOptionsConverter implements JsonCodec<AmqpReceiverOptions, JsonObject> {

  public static final AmqpReceiverOptionsConverter INSTANCE = new AmqpReceiverOptionsConverter();

  @Override public JsonObject encode(AmqpReceiverOptions value) { return (value != null) ? value.toJson() : null; }

  @Override public AmqpReceiverOptions decode(JsonObject value) { return (value != null) ? new AmqpReceiverOptions(value) : null; }

  @Override public Class<AmqpReceiverOptions> getTargetClass() { return AmqpReceiverOptions.class; }

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpReceiverOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "autoAcknowledgement":
          if (member.getValue() instanceof Boolean) {
            obj.setAutoAcknowledgement((Boolean)member.getValue());
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
        case "maxBufferedMessages":
          if (member.getValue() instanceof Number) {
            obj.setMaxBufferedMessages(((Number)member.getValue()).intValue());
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
    json.put("autoAcknowledgement", obj.isAutoAcknowledgement());
    if (obj.getCapabilities() != null) {
      JsonArray array = new JsonArray();
      obj.getCapabilities().forEach(item -> array.add(item));
      json.put("capabilities", array);
    }
    json.put("durable", obj.isDurable());
    json.put("dynamic", obj.isDynamic());
    if (obj.getLinkName() != null) {
      json.put("linkName", obj.getLinkName());
    }
    json.put("maxBufferedMessages", obj.getMaxBufferedMessages());
    if (obj.getQos() != null) {
      json.put("qos", obj.getQos());
    }
  }
}
