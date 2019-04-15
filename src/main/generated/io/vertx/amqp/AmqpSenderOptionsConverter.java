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

/**
 * Converter for {@link io.vertx.amqp.AmqpSenderOptions}.
 * NOTE: This class has been automatically generated from the {@link io.vertx.amqp.AmqpSenderOptions} original class using Vert.x codegen.
 */
public class AmqpSenderOptionsConverter {

  public static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, AmqpSenderOptions obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "autoDrained":
          if (member.getValue() instanceof Boolean) {
            obj.setAutoDrained((Boolean)member.getValue());
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

  public static void toJson(AmqpSenderOptions obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

  public static void toJson(AmqpSenderOptions obj, java.util.Map<String, Object> json) {
    json.put("autoDrained", obj.isAutoDrained());
    json.put("dynamic", obj.isDynamic());
    if (obj.getLinkName() != null) {
      json.put("linkName", obj.getLinkName());
    }
  }
}
