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

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * Configures the AMQP Receiver.
 */
@DataObject(generateConverter = true)
public class AmqpSenderOptions {

  private String linkName;
  private boolean dynamic;
  private boolean autoDrained = true;

  public AmqpSenderOptions() {

  }

  public AmqpSenderOptions(AmqpSenderOptions other) {
    this();
    setDynamic(other.isDynamic());
    setLinkName(other.getLinkName());
    setAutoDrained(other.isAutoDrained());
  }

  public AmqpSenderOptions(JsonObject json) {
    super();
    AmqpSenderOptionsConverter.fromJson(json, this);
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    AmqpSenderOptionsConverter.toJson(this, json);
    return json;
  }

  public String getLinkName() {
    return linkName;
  }

  public AmqpSenderOptions setLinkName(String linkName) {
    this.linkName = linkName;
    return this;
  }

  /**
   * @return whether the receiver is using a dynamic address.
   */
  public boolean isDynamic() {
    return dynamic;
  }

  /**
   * Sets whether the link remote terminus to be used should indicate it is
   * 'dynamic', requesting the peer names it with a dynamic address.
   * <p>
   * The address provided by the peer can then be inspected using the
   * {@link AmqpSender#address()} method on the {@link AmqpSender} received once opened.
   *
   * @param dynamic true if the link should request a dynamic terminus address
   * @return the options
   */
  public AmqpSenderOptions setDynamic(boolean dynamic) {
    this.dynamic = dynamic;
    return this;
  }

  /**
   * Get whether the link will automatically be marked drained after the send queue drain handler fires in drain mode.
   *
   * @return whether the link will automatically be drained after the send queue drain handler fires in drain mode
   * @see #setAutoDrained(boolean)
   */
  public boolean isAutoDrained() {
    return autoDrained;
  }

  /**
   * Sets whether the link is automatically marked drained after the send queue drain handler callback
   * returns if the receiving peer requested that credit be drained.
   * <p>
   * {@code true} by default.
   *
   * @param autoDrained whether the link will automatically be drained after the send queue drain handler fires in drain mode
   * @return the options
   */
  public AmqpSenderOptions setAutoDrained(boolean autoDrained) {
    this.autoDrained = autoDrained;
    return this;
  }
}
