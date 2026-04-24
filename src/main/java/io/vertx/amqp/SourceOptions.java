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
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;

/**
 * Configures an AMQP source terminus.
 * <p>
 * Maps to the AMQP 1.0 Source terminus properties: durability, expiry policy, timeout, and capabilities.
 */
@DataObject
@JsonGen(publicConverter = false)
public class SourceOptions {

  private String address;
  private String durability;
  private String expiryPolicy;
  private int timeout = -1;
  private List<String> capabilities;

  public SourceOptions() {
  }

  public SourceOptions(SourceOptions other) {
    this.address = other.address;
    this.durability = other.durability;
    this.expiryPolicy = other.expiryPolicy;
    this.timeout = other.timeout;
    if (other.capabilities != null) {
      this.capabilities = new ArrayList<>(other.capabilities);
    }
  }

  public SourceOptions(JsonObject json) {
    SourceOptionsConverter.fromJson(json, this);
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    SourceOptionsConverter.toJson(this, json);
    return json;
  }

  /**
   * Gets the source terminus address.
   *
   * @return the address, or {@code null} if not set.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Sets the source terminus address.
   *
   * @param address the address to set
   * @return the options
   */
  public SourceOptions setAddress(String address) {
    this.address = address;
    return this;
  }

  /**
   * Gets the source terminus durability.
   *
   * @return the durability, or {@code null} if not set.
   */
  public String getDurability() {
    return durability;
  }

  /**
   * Sets the source terminus durability.
   * <p>
   * Accepted values (case-insensitive): {@code NONE}, {@code CONFIGURATION}, {@code UNSETTLED_STATE}.
   *
   * @param durability the durability to set
   * @return the options
   * @throws IllegalArgumentException if the value does not match a valid durability
   */
  public SourceOptions setDurability(String durability) {
    this.durability = durability;
    return this;
  }

  /**
   * Gets the source terminus expiry policy.
   *
   * @return the expiry policy, or {@code null} if not set.
   */
  public String getExpiryPolicy() {
    return expiryPolicy;
  }

  /**
   * Sets the source terminus expiry policy.
   * <p>
   * Accepted values (case-insensitive): {@code LINK_DETACH}, {@code SESSION_END}, {@code CONNECTION_CLOSE}, {@code NEVER}.
   *
   * @param expiryPolicy the expiry policy to set
   * @return the options
   * @throws IllegalArgumentException if the value does not match a valid expiry policy
   */
  public SourceOptions setExpiryPolicy(String expiryPolicy) {
    this.expiryPolicy = expiryPolicy;
    return this;
  }

  /**
   * Gets the source terminus timeout in seconds.
   *
   * @return the timeout, or {@code -1} if not set.
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * Sets the source terminus timeout in seconds.
   * A value of {@code 0} means the terminus does not expire.
   * A value of {@code -1} (default) means not set.
   *
   * @param timeout the timeout in seconds
   * @return the options
   */
  public SourceOptions setTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  /**
   * Gets the list of capabilities to be set on the source terminus.
   *
   * @return the list of capabilities, or {@code null} if not set.
   */
  public List<String> getCapabilities() {
    return capabilities;
  }

  /**
   * Sets the list of capabilities to be set on the source terminus.
   *
   * @param capabilities the capabilities.
   * @return the options
   */
  public SourceOptions setCapabilities(List<String> capabilities) {
    this.capabilities = capabilities;
    return this;
  }

}
