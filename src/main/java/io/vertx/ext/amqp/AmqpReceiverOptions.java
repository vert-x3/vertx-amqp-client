/**
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
package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import io.vertx.proton.ProtonLink;

@DataObject(generateConverter = true)
public class AmqpReceiverOptions {

  private String linkName;
  private boolean dynamic;
  private String qos;

  public AmqpReceiverOptions() {

  }

  public AmqpReceiverOptions(AmqpReceiverOptions other) {
    this();
    setDynamic(other.isDynamic());
    setLinkName(other.getLinkName());
  }

  public AmqpReceiverOptions(JsonObject json) {
    super();
    AmqpReceiverOptionsConverter.fromJson(json, this);
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    AmqpReceiverOptionsConverter.toJson(this, json);
    return json;
  }


  public AmqpReceiverOptions setLinkName(String linkName) {
    this.linkName = linkName;
    return this;
  }

  public String getLinkName() {
    return linkName;
  }

  /**
   * Sets whether the link remote terminus to be used should indicate it is
   * 'dynamic', requesting the peer names it with a dynamic address.
   * The address provided by the peer can then be inspected using
   * {@link ProtonLink#getRemoteAddress()} (or inspecting the remote
   * terminus details directly) after the link has remotely opened.
   *
   * @param dynamic true if the link should request a dynamic terminus address
   * @return the options
   */
  public AmqpReceiverOptions setDynamic(boolean dynamic) {
    this.dynamic = dynamic;
    return this;
  }

  public boolean isDynamic() {
    return dynamic;
  }

  public String getQos() {
    return qos;
  }

  public AmqpReceiverOptions setQos(String qos) {
    this.qos = qos;
    return this;
  }
}
