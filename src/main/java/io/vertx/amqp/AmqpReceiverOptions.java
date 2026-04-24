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
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configures the AMQP Receiver.
 */
@DataObject
@JsonGen(publicConverter = false)
public class AmqpReceiverOptions {

  private String linkName;
  private boolean dynamic;
  private String qos;
  private List<String> capabilities = new ArrayList<>();
  private boolean durable;
  private int maxBufferedMessages;
  private boolean autoAcknowledgement = true;
  private boolean noLocal;
  private String selector;
  private Map<String, Object> linkProperties;
  private SourceOptions sourceOptions;
  private TargetOptions targetOptions;

  public AmqpReceiverOptions() {

  }

  public AmqpReceiverOptions(AmqpReceiverOptions other) {
    this();
    setDynamic(other.isDynamic());
    setLinkName(other.getLinkName());
    setCapabilities(other.getCapabilities());
    setDurable(other.isDurable());
    setMaxBufferedMessages(other.maxBufferedMessages);
    setNoLocal(other.noLocal);
    setSelector(other.selector);
    if (other.linkProperties != null) {
      setLinkProperties(new HashMap<>(other.linkProperties));
    }
    if (other.sourceOptions != null) {
      setSourceOptions(new SourceOptions(other.sourceOptions));
    }
    if (other.targetOptions != null) {
      setTargetOptions(new TargetOptions(other.targetOptions));
    }
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

  public String getLinkName() {
    return linkName;
  }

  public AmqpReceiverOptions setLinkName(String linkName) {
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
   * Sets whether the Source terminus to be used should specify it is 'dynamic',
   * requesting the peer creates a node and names it with a generated address.
   * <p>
   * The address provided by the peer can then be inspected using the
   * {@link AmqpReceiver#address()} method on the {@link AmqpReceiver} received once opened.
   *
   * @param dynamic true if the receiver should request dynamic creation of a node and address to consume from
   * @return the options
   */
  public AmqpReceiverOptions setDynamic(boolean dynamic) {
    this.dynamic = dynamic;
    return this;
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
   * Gets the list of capabilities to be set on the receiver source terminus.
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
   * Sets the list of capabilities to be set on the receiver source terminus.
   *
   * @param capabilities the set of source capabilities.
   * @return the options
   */
  public AmqpReceiverOptions setCapabilities(List<String> capabilities) {
    this.capabilities = capabilities;
    return this;
  }

  /**
   * Adds a capability to be set on the receiver source terminus.
   *
   * @param capability the source capability to add, must not be {@code null}
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
   * @return the max buffered messages
   */
  public int getMaxBufferedMessages() {
    return this.maxBufferedMessages;
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
   * @return {@code true} if the auto-acknowledgement is enabled, {@code false} otherwise.
   */
  public boolean isAutoAcknowledgement() {
    return autoAcknowledgement;
  }

  /**
   * Sets the auto-acknowledgement.
   * When enabled (default), the messages are automatically acknowledged. If set to {@code false}, the messages must
   * be acknowledged explicitly using {@link AmqpMessage#accepted()}, {@link AmqpMessage#released()} and
   * {@link AmqpMessage#rejected()}.
   *
   * @param auto whether or not the auto-acknowledgement should be enabled.
   * @return the current {@link AmqpReceiverOptions} instance.
   */
  public AmqpReceiverOptions setAutoAcknowledgement(boolean auto) {
    this.autoAcknowledgement = auto;
    return this;
  }

  /**
   * @return the message selector String
   */
  public String getSelector() {
    return selector;
  }

  /**
   * Sets a message selector string.
   *
   * Used to define an "apache.org:selector-filter:string" filter on the source terminus, using SQL-based syntax to request
   * the server filters which messages are delivered to the receiver (if supported by the server in question). Precise
   * functionality supported and syntax needed can vary depending on the server.
   *
   * @param selector the selector string to set.
   */
  public AmqpReceiverOptions setSelector(final String selector) {
    this.selector = selector;
    return this;
  }

  /**
   * @return whether this receiver should not receive messages that were sent on the same underlying connection
   */
  public boolean isNoLocal() {
    return noLocal;
  }

  /**
   * Sets whether this receiver should not receive messages that were sent using the same underlying connection.
   *
   * Used to determine whether to define an "apache.org:no-local-filter:list" filter on the source terminus, requesting
   * that the server filters which messages are delivered to the receiver so that they do not include messages sent on
   * the same underlying connection (if supported by the server in question).
   *
   * @param noLocal true if this receiver shall not receive messages that were sent on the same underlying connection
   */
  public AmqpReceiverOptions setNoLocal(final boolean noLocal) {
    this.noLocal = noLocal;
    return this;
  }

  /**
   * Gets the link properties to be sent in the AMQP attach frame.
   *
   * @return the link properties map, or {@code null} if not set.
   */
  public Map<String, Object> getLinkProperties() {
    return linkProperties;
  }

  /**
   * Sets link properties to be sent in the AMQP attach frame.
   * Keys are converted to AMQP Symbol type when applied.
   *
   * @param linkProperties the link properties map
   * @return the options
   */
  public AmqpReceiverOptions setLinkProperties(Map<String, Object> linkProperties) {
    this.linkProperties = linkProperties;
    return this;
  }

  /**
   * Gets the source terminus options.
   *
   * @return the source options, or {@code null} if not set.
   */
  public SourceOptions getSourceOptions() {
    return sourceOptions;
  }

  /**
   * Sets the source terminus options for fine-grained control over durability, expiry policy, timeout, and capabilities.
   * <p>
   * If {@link SourceOptions#getDurability()} or {@link SourceOptions#getExpiryPolicy()} is set,
   * it overrides the values implied by the {@link #setDurable(boolean)} convenience option
   * (which sets durability to {@code UNSETTLED_STATE} and expiry policy to {@code NEVER}).
   * <p>
   * If {@link SourceOptions#getCapabilities()} is set, it overrides the capabilities set via
   * {@link #setCapabilities(List)} and {@link #addCapability(String)}.
   * <p>
   * If {@link SourceOptions#getAddress()} is set, it overrides the address passed to
   * {@link AmqpConnection#createReceiver(String, AmqpReceiverOptions)}.
   * It may also conflict with the {@link #setDynamic(boolean)} option, which requests the peer to
   * generate the source address.
   *
   * @param sourceOptions the source terminus options
   * @return the options
   */
  public AmqpReceiverOptions setSourceOptions(SourceOptions sourceOptions) {
    this.sourceOptions = sourceOptions;
    return this;
  }

  /**
   * Gets the target terminus options.
   *
   * @return the target options, or {@code null} if not set.
   */
  public TargetOptions getTargetOptions() {
    return targetOptions;
  }

  /**
   * Sets the target terminus options for fine-grained control over the receiver's target terminus.
   * <p>
   * If {@link TargetOptions#getAddress()} is set, it sets the target address on the receiver link.
   *
   * @param targetOptions the target terminus options
   * @return the options
   */
  public AmqpReceiverOptions setTargetOptions(TargetOptions targetOptions) {
    this.targetOptions = targetOptions;
    return this;
  }
}
