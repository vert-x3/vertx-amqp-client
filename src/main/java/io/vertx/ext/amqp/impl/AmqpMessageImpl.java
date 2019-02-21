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
package io.vertx.ext.amqp.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.amqp.AmqpMessage;
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
  private ReplyManager replySender;

  public AmqpMessageImpl(Message message, ProtonDelivery delivery) {
    this.message = message;
    this.delivery = delivery;
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

  @Override
  public boolean isBodyNull() {
    return message.getBody() == null || getAmqpValue() == null;
  }

  private Object getAmqpValue() {
    if (message.getBody().getType() != Section.SectionType.AmqpValue) {
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

  /**
   * @noinspection unchecked
   */
  @Override
  public <T> List<T> bodyAsList() {
    Section body = message.getBody();
    if (body.getType() == Section.SectionType.AmqpSequence) {
      return (List<T>) ((AmqpSequence) message.getBody()).getValue();
    } else {
      Object value = getAmqpValue();
      if (value instanceof List) {
        return (List<T>) value;
      }
      throw new IllegalStateException("Cannot extract a list from the message body");
    }
  }

  /**
   * @noinspection unchecked
   */
  @Override
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
    return message.getContentType();
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
  public long deliveryCount() {
    return message.getDeliveryCount();
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
    return JsonObject.mapFrom(properties.getValue());
  }

  @Override
  public Message unwrap() {
    return message;
  }

  public void delivered() {
    if (delivery != null) {
      ProtonHelper.accepted(delivery, true);
    }
  }

  @Override
  public synchronized AmqpMessage reply(AmqpMessage message) {
    if (replySender != null) {
      replySender.sendReply(this, message);
    } else {
      throw new IllegalStateException("Cannot reply to message, no reply sender");
    }
    return this;
  }

  @Override
  public AmqpMessage reply(AmqpMessage message, Handler<AsyncResult<AmqpMessage>> replyToReplyHandler) {
    if (replySender != null) {
      replySender.sendReply(this, message, replyToReplyHandler);
    } else {
      throw new IllegalStateException("Cannot reply to message, no reply sender");
    }
    return this;
  }

  public synchronized void setReplyManager(ReplyManager sender) {
    this.replySender = sender;
  }
}
