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
package io.vertx.amqp.impl;

import io.vertx.amqp.AmqpMessage;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonHelper;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AmqpMessageImpl implements AmqpMessage {
  private final Message message;
  private final ProtonDelivery delivery;
  private AmqpConnectionImpl connection;

  public AmqpMessageImpl(Message message, ProtonDelivery delivery, AmqpConnectionImpl connection) {
    this.message = message;
    this.delivery = delivery;
    this.connection = connection;
  }

  public AmqpMessageImpl(Message message) {
    this.message = message;
    this.delivery = null;
  }

  @Override
  public boolean isDurable() {
    return message.isDurable();
  }

  @Override
  public boolean isFirstAcquirer() {
    return message.isFirstAcquirer();
  }

  @Override
  public int priority() {
    return message.getPriority();
  }

  @Override
  public String id() {
    Object id = message.getMessageId();
    if (id != null) {
      return id.toString();
    }
    return null;
  }

  @Override
  public String address() {
    return message.getAddress();
  }

  @Override
  public String replyTo() {
    return message.getReplyTo();
  }

  @Override
  public String correlationId() {
    Object id = message.getCorrelationId();
    if (id != null) {
      return id.toString();
    }
    return null;
  }

  private boolean isBodyAmqpValue() {
    final Section body = message.getBody();
    return body != null && body.getType() == Section.SectionType.AmqpValue;
  }

  @Override
  public boolean isBodyNull() {
    final Section body = message.getBody();
    return body == null || isBodyAmqpValue() && ((AmqpValue) body).getValue() == null;
  }

  private Object getAmqpValue() {
    if (!isBodyAmqpValue()) {
      throw new IllegalStateException("The body is not an AMQP Value");
    }
    return ((AmqpValue) message.getBody()).getValue();
  }

  @Override
  public boolean bodyAsBoolean() {
    return (boolean) getAmqpValue();
  }

  @Override
  public byte bodyAsByte() {
    return (byte) getAmqpValue();
  }

  @Override
  public short bodyAsShort() {
    return (short) getAmqpValue();
  }

  @Override
  public int bodyAsInteger() {
    return (int) getAmqpValue();
  }

  @Override
  public long bodyAsLong() {
    return (long) getAmqpValue();
  }

  @Override
  public float bodyAsFloat() {
    return (float) getAmqpValue();
  }

  @Override
  public double bodyAsDouble() {
    return (double) getAmqpValue();
  }

  @Override
  public char bodyAsChar() {
    return (char) getAmqpValue();
  }

  @Override
  public Instant bodyAsTimestamp() {
    Object value = getAmqpValue();
    if (!(value instanceof Date)) {
      throw new IllegalStateException("Expecting a Date object, got a " + value);
    }
    return ((Date) value).toInstant();
  }

  @Override
  public UUID bodyAsUUID() {
    return (UUID) getAmqpValue();
  }

  @Override
  public Buffer bodyAsBinary() {
    Section body = message.getBody();
    if (body.getType() != Section.SectionType.Data) {
      throw new IllegalStateException("The body is not of type 'data'");
    }
    byte[] bytes = ((Data) message.getBody()).getValue().getArray();
    return Buffer.buffer(bytes);
  }

  @Override
  public String bodyAsString() {
    return (String) getAmqpValue();
  }

  @Override
  public String bodyAsSymbol() {
    Object value = getAmqpValue();
    if (value instanceof Symbol) {
      return ((Symbol) value).toString();
    }
    throw new IllegalStateException("Expected a Symbol, got a " + value.getClass());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> bodyAsList() {
    Section body = message.getBody();
    if (body.getType() == Section.SectionType.AmqpSequence) {
      return ((AmqpSequence) message.getBody()).getValue();
    } else {
      Object value = getAmqpValue();
      if (value instanceof List) {
        return (List<T>) value;
      }
      throw new IllegalStateException("Cannot extract a list from the message body");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> Map<K, V> bodyAsMap() {
    Object value = getAmqpValue();
    if (value instanceof Map) {
      return (Map<K, V>) value;
    }
    throw new IllegalStateException("Cannot extract a map from the message body");
  }

  @Override
  public JsonObject bodyAsJsonObject() {
    return bodyAsBinary().toJsonObject();
  }

  @Override
  public JsonArray bodyAsJsonArray() {
    return bodyAsBinary().toJsonArray();
  }

  @Override
  public String subject() {
    return message.getSubject();
  }

  @Override
  public String contentType() {
    return message.getContentType();
  }

  @Override
  public String contentEncoding() {
    return message.getContentEncoding();
  }

  @Override
  public long expiryTime() {
    return message.getExpiryTime();
  }

  @Override
  public long creationTime() {
    return message.getCreationTime();
  }

  @Override
  public long ttl() {
    return message.getTtl();
  }

  @Override
  public int deliveryCount() {
    return (int) message.getDeliveryCount();
  }

  @Override
  public String groupId() {
    return message.getGroupId();
  }

  @Override
  public String replyToGroupId() {
    return message.getReplyToGroupId();
  }

  @Override
  public long groupSequence() {
    return message.getGroupSequence();
  }

  @Override
  public JsonObject applicationProperties() {
    ApplicationProperties properties = message.getApplicationProperties();
    if (properties == null) {
      return null;
    }
    return new JsonObject(properties.getValue());
  }

  @Override
  public Message unwrap() {
    return message;
  }

  @Override
  public AmqpMessage accepted() {
    if (delivery != null) {
      connection.runWithTrampoline(v -> ProtonHelper.accepted(delivery, true));
    } else {
      throw new IllegalStateException("The message is not a received message");
    }
    return this;
  }

  @Override
  public AmqpMessage rejected() {
    if (delivery != null) {
      connection.runWithTrampoline(v -> ProtonHelper.rejected(delivery, true));
    } else {
      throw new IllegalStateException("The message is not a received message");
    }
    return this;
  }

  @Override
  public AmqpMessage released() {
    if (delivery != null) {
      connection.runWithTrampoline(v -> ProtonHelper.released(delivery, true));
    } else {
      throw new IllegalStateException("The message is not a received message");
    }
    return this;
  }

  @Override
  public AmqpMessage modified(boolean deliveryFailed, boolean undeliverableHere) {
    if (delivery != null) {
      connection.runWithTrampoline(v -> ProtonHelper.modified(delivery, true, deliveryFailed, undeliverableHere));
    } else {
      throw new IllegalStateException("The message is not a received message");
    }
    return this;
  }
}
