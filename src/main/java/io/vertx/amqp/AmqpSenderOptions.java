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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * Configures the AMQP Sender.
 */
@DataObject(generateConverter = true)
public class AmqpSenderOptions {

  private String linkName;
  private boolean dynamic;
  private boolean autoDrained = true;
  private List<String> capabilities = new ArrayList<>();

  public AmqpSenderOptions() {
  }

  public AmqpSenderOptions(AmqpSenderOptions other) {
    setDynamic(other.isDynamic());
    setLinkName(other.getLinkName());
    setAutoDrained(other.isAutoDrained());
  }

  public AmqpSenderOptions(JsonObject json) {
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
   * @return whether the sender is using a dynamic address.
   */
  public boolean isDynamic() {
    return dynamic;
  }

  /**
   * Sets whether the Target terminus to be used should specify it is 'dynamic',
   * requesting the peer creates a node and names it with a generated address.
   * <p>
   * The address provided by the peer can then be inspected using the
   * {@link AmqpSender#address()} method on the {@link AmqpSender} received once opened.
   *
   * @param dynamic true if the sender should request dynamic creation of a node and address to send to
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

  /**
   * Gets the list of capabilities to be set on the sender target terminus.
   *
   * @return the list of capabilities, empty if none.
   */
  public List<String> getCapabilities() {
    if (capabilities == null) {
      return new ArrayList<>();
    }
    return capabilities;
  }

  /**
   * Sets the list of capabilities to be set on the sender target terminus.
   *
   * @param capabilities the set of target capabilities.
   * @return the options
   */
  public AmqpSenderOptions setCapabilities(List<String> capabilities) {
    this.capabilities = capabilities;
    return this;
  }

  /**
   * Adds a capability to be set on the sender target terminus.
   *
   * @param capability the target capability to add, must not be {@code null}
   * @return the options
   */
  public AmqpSenderOptions addCapability(String capability) {
    Objects.requireNonNull(capability, "The capability must not be null");
    if (this.capabilities == null) {
      this.capabilities = new ArrayList<>();
    }
    this.capabilities.add(capability);
    return this;
  }
}
