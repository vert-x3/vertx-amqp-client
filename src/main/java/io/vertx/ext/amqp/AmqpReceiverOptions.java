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
package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Configures the AMQP Receiver.
 */
@DataObject(generateConverter = true)
public class AmqpReceiverOptions {

  private String linkName;
  private boolean dynamic;
  private String qos;
  private List<String> capabilities = new ArrayList<>();
  private boolean durable;
  private String terminusDurability;
  private String terminusExpiryPolicy;
  private int maxBufferedMessages;

  public AmqpReceiverOptions() {

  }

  public AmqpReceiverOptions(AmqpReceiverOptions other) {
    this();
    setDynamic(other.isDynamic());
    setLinkName(other.getLinkName());
    setCapabilities(other.getCapabilities());
    setDurable(other.isDurable());
    setTerminusDurability(other.getTerminusDurability());
    setTerminusExpiryPolicy(other.getTerminusExpiryPolicy());
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
   * <p>
   * The address provided by the peer can then be inspected using the
   * {@link AmqpReceiver#address()} method on the {@link AmqpReceiver} received once opened.
   *
   * @param dynamic true if the link should request a dynamic terminus address
   * @return the options
   */
  public AmqpReceiverOptions setDynamic(boolean dynamic) {
    this.dynamic = dynamic;
    return this;
  }

  /**
   * @return whether the receiver is using a dynamic address.
   */
  public boolean isDynamic() {
    return dynamic;
  }

  /**
   * Gets the local QOS config, values can be {@code null}, {@code AT_MOST_ONCE} or {@code AT_LEAST_ONCE}.
   *
   * @return the local QOS config.
   */
  public String getQos() {
    return qos;
  }

  /**
   * Sets the local QOS config.
   *
   * @param qos the local QOS config. Accepted values are: {@code null}, {@code AT_MOST_ONCE} or {@code AT_LEAST_ONCE}.
   * @return the options.
   */
  public AmqpReceiverOptions setQos(String qos) {
    this.qos = qos;
    return this;
  }

  /**
   * Gets the list of desired capabilities for the source.
   * A registry of commonly defined source capabilities and their meanings is maintained at
   * <a href="http://www.amqp.org/specification/1.0/source-capabilities">AMQP Source Capabilities</a>.
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
   * Sets the list of desired capabilities
   * A registry of commonly defined source capabilities and their meanings is maintained at
   * <a href="http://www.amqp.org/specification/1.0/source-capabilities">AMQP Source Capabilities</a>.
   *
   * @param capabilities the set of capabilities.
   * @return the options
   */
  public AmqpReceiverOptions setCapabilities(List<String> capabilities) {
    this.capabilities = capabilities;
    return this;
  }

  /**
   * Adds a desired capability.
   * A registry of commonly defined source capabilities and their meanings is maintained at
   * <a href="http://www.amqp.org/specification/1.0/source-capabilities">AMQP Source Capabilities</a>.
   *
   * @param capability the capability to add, must not be {@code null}
   * @return the options
   */
  public AmqpReceiverOptions addCapability(String capability) {
    Objects.requireNonNull(capability, "The capability must not be null");
    if (this.capabilities == null) {
      this.capabilities = new ArrayList<>();
    }
    this.capabilities.add(capability);
    return this;
  }

  /**
   * @return if the receiver is durable.
   */
  public boolean isDurable() {
    return durable;
  }

  /**
   * Sets the durability.
   * <p>
   * Passing {@code true} sets the expiry policy of the source to {@code NEVER} and the durability of the source
   * to {@code UNSETTLED_STATE}.
   *
   * @param durable whether or not the receiver must indicate it's durable
   * @return the options.
   */
  public AmqpReceiverOptions setDurable(boolean durable) {
    this.durable = durable;
    return this;
  }

  /**
   * Indicates what state of the terminus is retained durably: the state of durable messages (@{code UNSETTLED_STATE}),
   * only existence and configuration of the terminus ({@code CONFIGURATION}), or no state at all ({@code NONE}).
   *
   * @return Indicates what state of the terminus is retained durably, or {@code null} if not set
   */
  public String getTerminusDurability() {
    return terminusDurability;
  }

  /**
   * Sets what state of the terminus is retained durably. Accepted values are: state of durable messages (@{code UNSETTLED_STATE}),
   * only existence and configuration of the terminus ({@code CONFIGURATION}), or no state at all ({@code NONE}).
   *
   * @param terminusDurability the state, or {@code null}
   * @return the options
   */
  public AmqpReceiverOptions setTerminusDurability(String terminusDurability) {
    this.terminusDurability = terminusDurability;
    return this;
  }

  /**
   * Gets the policy that determines when the expiry timer of a terminus starts counting down from the timeout value.
   * Values can be:
   * <ul>
   * <li>{@code null} - Not set</li>
   * <li>{@code link-detach} - The expiry timer starts when terminus is detached.</li>
   * <li>{@code session-end} - The expiry timer starts when the most recently associated session is ended.</li>
   * <li>{@code connection-close} - The expiry timer starts when most recently associated connection is closed.</li>
   * <li>{@code never} - The terminus never expires.</li>
   * </ul>
   *
   * @return the terminus expiry policy
   */
  public String getTerminusExpiryPolicy() {
    return terminusExpiryPolicy;
  }

  /**
   * Sets the policy that determines when the expiry timer of a terminus starts counting down from the timeout value.
   * Values can be:
   * <ul>
   * <li>{@code null} - Not set</li>
   * <li>{@code link-detach} - The expiry timer starts when terminus is detached.</li>
   * <li>{@code session-end} - The expiry timer starts when the most recently associated session is ended.</li>
   * <li>{@code connection-close} - The expiry timer starts when most recently associated connection is closed.</li>
   * <li>{@code never} - The terminus never expires.</li>
   * </ul>
   *
   * @param terminusExpiryPolicy the policy
   * @return the options
   */
  public AmqpReceiverOptions setTerminusExpiryPolicy(String terminusExpiryPolicy) {
    this.terminusExpiryPolicy = terminusExpiryPolicy;
    return this;
  }

  /**
   * Sets the max buffered messages. This message can be used to configure the initial credit of a receiver.
   *
   * @param maxBufferSize the max buffered size, must be positive. If not set, default credit is used.
   * @return the current {@link AmqpReceiverOptions} instance.
   */
  public AmqpReceiverOptions setMaxBufferedMessages(int maxBufferSize) {
    this.maxBufferedMessages = maxBufferSize;
    return this;
  }

  /**
   * @return the max buffered messages
   */
  public int getMaxBufferedMessages() {
    return this.maxBufferedMessages;
  }
}
