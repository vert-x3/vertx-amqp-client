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
package io.vertx.ext.amqp.impl;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.amqp.AmqpMessage;
import io.vertx.ext.amqp.AmqpMessageBuilder;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AmqpMessageBuilderImpl implements AmqpMessageBuilder {

  private Message message;

  public AmqpMessageBuilderImpl() {
    message = Message.Factory.create();
  }

  public AmqpMessageBuilderImpl(AmqpMessage existing) {
    message = existing.unwrap();
  }

  public AmqpMessageBuilderImpl(Message existing) {
    message = existing;
  }


  @Override
  public AmqpMessage build() {
    return new AmqpMessageImpl(message);
  }

  @Override
  public AmqpMessageBuilderImpl priority(short priority) {
    message.setPriority(priority);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl id(String id) {
    message.setMessageId(id);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl address(String address) {
    message.setAddress(address);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl replyTo(String replyTo) {
    message.setReplyTo(replyTo);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl correlationId(String correlationId) {
    message.setCorrelationId(correlationId);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withBody(String value) {
    message.setBody(new AmqpValue(value));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withSymbolAsBody(String value) {
    message.setBody(new AmqpValue(Symbol.valueOf(value)));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl subject(String subject) {
    message.setSubject(subject);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl contentType(String ct) {
    message.setContentType(ct);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl contentEncoding(String ct) {
    message.setContentEncoding(ct);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl expiryTime(long expiry) {
    message.setExpiryTime(expiry);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl creationTime(long ct) {
    message.setCreationTime(ct);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl ttl(long ttl) {
    message.setTtl(ttl);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl groupId(String gi) {
    message.setGroupId(gi);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl replyToGroupId(String rt) {
    message.setReplyToGroupId(rt);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl applicationProperties(JsonObject props) {
    ApplicationProperties properties = new ApplicationProperties(props.getMap());
    message.setApplicationProperties(properties);
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withBooleanAsBody(boolean v) {
    message.setBody(new AmqpValue(v));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withByteAsBody(byte v) {
    message.setBody(new AmqpValue(v));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withShortAsBody(short v) {
    message.setBody(new AmqpValue(v));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withIntegerAsBody(int v) {
    message.setBody(new AmqpValue(v));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withLongAsBody(long v) {
    message.setBody(new AmqpValue(v));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withFloatAsBody(float v) {
    message.setBody(new AmqpValue(v));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withDoubleAsBody(double v) {
    message.setBody(new AmqpValue(v));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withCharAsBody(char c) {
    message.setBody(new AmqpValue(c));
    return this;
  }

  @Override
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  public AmqpMessageBuilderImpl withInstantAsBody(Instant v) {
    message.setBody(new AmqpValue(Date.from(v)));
    return this;
  }

  @Override
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  public AmqpMessageBuilderImpl withUuidAsBody(UUID v) {
    message.setBody(new AmqpValue(v));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withListAsBody(List list) {
    message.setBody(new AmqpValue(list));
    return this;
  }

  @Override
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  public AmqpMessageBuilderImpl withMapAsBody(Map map) {
    message.setBody(new AmqpValue(map));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withBufferAsBody(Buffer buffer) {
    message.setBody(new Data(new Binary(buffer.getBytes())));
    return this;
  }

  @Override
  public AmqpMessageBuilderImpl withJsonObjectAsBody(JsonObject json) {
    return contentType("application/json")
      .withBufferAsBody(json.toBuffer());
  }

  @Override
  public AmqpMessageBuilderImpl withJsonArrayAsBody(JsonArray json) {
    return contentType("application/json")
      .withBufferAsBody(json.toBuffer());
  }
}
