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

import io.vertx.amqp.impl.AmqpMessageBuilderImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.qpid.proton.message.Message;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Represents an AMQP message.
 * <p>
 * Reference about the different metadata can be found on
 * <a href="http://docs.oasis-open.org/amqp/core/v1.0/amqp-core-messaging-v1.0.html#type-properties">AMQP message properties</a>.
 * <p>
 * Note that the body is retrieved using {@code body*} method depending on the expected type.
 */
@VertxGen
public interface AmqpMessage {

  /**
   * @return a builder to create an {@link AmqpMessage}.
   */
  static AmqpMessageBuilder create() {
    return new AmqpMessageBuilderImpl();
  }

  /**
   * Creates a builder to create a new {@link AmqpMessage} copying the metadata from the passed message.
   *
   * @param existing an existing message, must not be {@code null}.
   * @return a builder to create an {@link AmqpMessage}.
   */
  static AmqpMessageBuilder create(AmqpMessage existing) {
    return new AmqpMessageBuilderImpl(existing);
  }

  /**
   * Creates a builder to create a new {@link AmqpMessage} copying the metadata from the passed (Proton) message.
   *
   * @param existing an existing (Proton) message, must not be {@code null}.
   * @return a builder to create an {@link AmqpMessage}.
   */
  @GenIgnore
  static AmqpMessageBuilder create(Message existing) {
    return new AmqpMessageBuilderImpl(existing);
  }

  /**
   * @return whether or not the message is durable.
   * @see <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP specification</a>
   */
  boolean isDurable();

  /**
   * @return if {@code true}, then this message has not been acquired by any other link. If {@code false}, then this
   * message MAY have previously been acquired by another link or links.
   * @see <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP specification</a>
   */
  boolean isFirstAcquirer();

  /**
   * @return the relative message priority. Higher numbers indicate higher priority messages. Messages with higher
   * priorities MAY be delivered before those with lower priorities.
   * @see <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP specification</a>
   */
  int priority();

  /**
   * @return the number of unsuccessful previous attempts to deliver this message. If this value is non-zero it can be
   * taken as an indication that the delivery might be a duplicate. On first delivery, the value is zero. It is
   * incremented upon an outcome being settled at the sender, according to rules defined for each outcome.
   * @see <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-header">AMQP specification</a>
   */
  int deliveryCount();

  /**
   * @return the duration in milliseconds for which the message is to be considered "live".
   * @see <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-properties">AMQP specification</a>
   */
  long ttl();

  /**
   * @return the message id
   * @see <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-properties">AMQP specification</a>
   */
  String id();

  /**
   * @return the message address, also named {@code to} field
   * @see <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-properties">AMQP specification</a>
   */
  String address();

  /**
   * @return The address of the node to send replies to, if any.
   * @see <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-properties">AMQP specification</a>
   */
  String replyTo();

  /**
   * @return The client-specific id that can be used to mark or identify messages between clients.
   * @see <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-properties">AMQP specification</a>
   */
  String correlationId();

  /**
   * @return whether the body is {@code null}. This method returns {@code true} is the message does not contain a body or
   * if the message contain a {@code null} AMQP value as body.
   */
  boolean isBodyNull();

  /**
   * @return the boolean value contained in the body. The value must be passed as AMQP value.
   */
  boolean bodyAsBoolean();

  /**
   * @return the byte value contained in the body. The value must be passed as AMQP value.
   */
  byte bodyAsByte();

  /**
   * @return the short value contained in the body. The value must be passed as AMQP value.
   */
  short bodyAsShort();

  /**
   * @return the integer value contained in the body. The value must be passed as AMQP value.
   */
  int bodyAsInteger();

  /**
   * @return the long value contained in the body. The value must be passed as AMQP value.
   */
  long bodyAsLong();

  /**
   * @return the float value contained in the body. The value must be passed as AMQP value.
   */
  float bodyAsFloat();

  /**
   * @return the double value contained in the body. The value must be passed as AMQP value.
   */
  double bodyAsDouble();

  /**
   * @return the character value contained in the body. The value must be passed as AMQP value.
   */
  char bodyAsChar();

  /**
   * @return the timestamp value contained in the body. The value must be passed as AMQP value.
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Instant bodyAsTimestamp();

  /**
   * @return the UUID value contained in the body. The value must be passed as AMQP value.
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  UUID bodyAsUUID();

  /**
   * @return the bytes contained in the body. The value must be passed as AMQP data.
   */
  Buffer bodyAsBinary();

  /**
   * @return the string value contained in the body. The value must be passed as AMQP value.
   */
  String bodyAsString();

  /**
   * @return the symbol value contained in the body. The value must be passed as AMQP value.
   */
  String bodyAsSymbol();

  /**
   * @return the list of values contained in the body. The value must be passed as AMQP value.
   */
  <T> List<T> bodyAsList();

  /**
   * @return the map contained in the body. The value must be passed as AMQP value.
   */
  @GenIgnore <K, V> Map<K, V> bodyAsMap();

  /**
   * @return the JSON object contained in the body. The value must be passed as AMQP data.
   */
  JsonObject bodyAsJsonObject();

  /**
   * @return the JSON array contained in the body. The value must be passed as AMQP data.
   */
  JsonArray bodyAsJsonArray();

  String subject();

  String contentType();

  String contentEncoding();

  long expiryTime();

  long creationTime();

  String groupId();

  String replyToGroupId();

  long groupSequence();

  /**
   * @return the message properties as JSON object.
   */
  JsonObject applicationProperties();

  @GenIgnore
  Message unwrap();

  /**
   * When receiving a message, and when auto-acknowledgement is disabled, this method is used to acknowledge
   * the incoming message. It marks the message as delivered with the {@code accepted} status.
   *
   * @return the current {@link AmqpMessage} object
   * @throws IllegalStateException is the current message is not a received message.
   */
  @Fluent
  AmqpMessage accepted();

  /**
   * When receiving a message, and when auto-acknowledgement is disabled, this method is used to acknowledge
   * the incoming message as {@code rejected}.
   *
   * @return the current {@link AmqpMessage} object
   * @throws IllegalStateException is the current message is not a received message.
   */
  @Fluent
  AmqpMessage rejected();

  /**
   * When receiving a message, and when auto-acknowledgement is disabled, this method is used to acknowledge
   * the incoming message as {@code released}.
   *
   * @return the current {@link AmqpMessage} object
   * @throws IllegalStateException is the current message is not a received message.
   */
  @Fluent
  AmqpMessage released();


  //TODO What type should we use for delivery annotations and message annotations

}
