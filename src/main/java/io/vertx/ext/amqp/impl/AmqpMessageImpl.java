package io.vertx.ext.amqp.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.amqp.AmqpMessage;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

import java.util.List;

public class AmqpMessageImpl implements AmqpMessage {
  private final Message message;

  public AmqpMessageImpl(Message message) {
    this.message = message;
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
  public Buffer body() {
    switch (message.getBody().getType()) {
      case Data:
        Data data = (Data) message.getBody();
        return Buffer.buffer(data.getValue().getArray());
      case AmqpValue:
        AmqpValue value = (AmqpValue) message.getBody();
        return asBuffer(value.getValue());
      case AmqpSequence:
        List list = ((AmqpSequence) message.getBody()).getValue();
        return Json.encodeToBuffer(list);
      default:
        throw new UnsupportedOperationException("Not yet accepted type: " + message.getBody().getType());
    }
  }

  private Buffer asBuffer(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return Buffer.buffer((String) value);
    }
    return Json.encodeToBuffer(value);
  }

  @Override
  public <T> T bodyAs(Class<T> target) {
    throw new UnsupportedOperationException("NOT YET SUPPORTED");
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
    return JsonObject.mapFrom(message.getApplicationProperties().getValue());
  }

  @Override
  public Message unwrap() {
    return message;
  }


}
