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
 * Converter and mapper for {@link io.vertx.amqp.AmqpSenderOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqp.AmqpSenderOptions} original class using Vert.x codegen.
 */
public class AmqpSenderOptionsConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpSenderOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
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
      }
    }
  }

   static void toJson(AmqpSenderOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(AmqpSenderOptions obj, java.util.Map<String, Object> json) {
    json.put("autoDrained", obj.isAutoDrained());
    if (obj.getCapabilities() != null) {
      JsonArray array = new JsonArray();
      obj.getCapabilities().forEach(item -> array.add(item));
      json.put("capabilities", array);
    }
    json.put("dynamic", obj.isDynamic());
    if (obj.getLinkName() != null) {
      json.put("linkName", obj.getLinkName());
    }
  }
}
