package io.vertx.ext.amqp.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.amqp.AmqpMessage;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
  public boolean isBodyNull() {
    return  message.getBody() == null || getAmqpValue() == null;
  }

  private Object getAmqpValue() {
    if (message.getBody().getType() != Section.SectionType.AmqpValue) {
      throw new IllegalStateException("The body is not an AMQP Value");
    }
    return ((AmqpValue) message.getBody()).getValue();
  }

  @Override
  public boolean getBodyAsBoolean() {
    return (boolean) getAmqpValue();
  }

  @Override
  public byte getBodyAsByte() {
    return (byte) getAmqpValue();
  }

  @Override
  public short getBodyAsShort() {
    return (short) getAmqpValue();
  }

  @Override
  public int getBodyAsInteger() {
    return (int) getAmqpValue();
  }

  @Override
  public long getBodyAsLong() {
    return (long) getAmqpValue();
  }

  @Override
  public float getBodyAsFloat() {
    return (float) getAmqpValue();
  }

  @Override
  public double getBodyAsDouble() {
    return (double) getAmqpValue();
  }

  @Override
  public char getBodyAsChar() {
    return (char) getAmqpValue();
  }

  @Override
  public Instant getBodyAsTimestamp() {
    Object value = getAmqpValue();
    if (!(value instanceof Date)) {
      throw new IllegalStateException("Expecting a Date object, got a " + value);
    }
    return ((Date) value).toInstant();
  }

  @Override
  public UUID getBodyAsUUID() {
    return (UUID) getAmqpValue();
  }

  @Override
  public Buffer getBodyAsBinary() {
    Section body = message.getBody();
    if (body.getType() != Section.SectionType.Data) {
      throw new IllegalStateException("The body is not of type 'data'");
    }
    byte[] bytes = ((Data) message.getBody()).getValue().getArray();
    return Buffer.buffer(bytes);
  }

  @Override
  public String getBodyAsString() {
    return (String) getAmqpValue();
  }

  @Override
  public String getBodyAsSymbol() {
    // TODO To be checked
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
  public <T> List<T> getBodyAsList() {
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
  public <K,V> Map<K, V> getBodyAsMap() {
    Object value = getAmqpValue();
    if (value instanceof Map) {
      return (Map<K, V>) value;
    }
    throw new IllegalStateException("Cannot extract a map from the message body");
  }


  @Override
  public JsonObject getBodyAsJsonObject() {
    return getBodyAsBinary().toJsonObject();
  }

  @Override
  public JsonArray getBodyAsJsonArray() {
    return getBodyAsBinary().toJsonArray();
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
