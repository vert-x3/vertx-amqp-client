package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.amqp.impl.AmqpMessageImpl;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.message.Message;

@GenIgnore
public class AmqpMessageBuilder {

  private Message message;

  public AmqpMessageBuilder() {
    message = Message.Factory.create();
  }

  public AmqpMessageBuilder(AmqpMessage existing) {
    message = existing.unwrap();
  }

  public AmqpMessageBuilder(Message existing) {
    message = existing;
  }

  public AmqpMessage build() {
    return new AmqpMessageImpl(message);
  }


  public AmqpMessageBuilder priority(short priority) {
    message.setPriority(priority);
    return this;
  }

  public AmqpMessageBuilder id(String id) {
    message.setMessageId(id);
    return this;
  }

  public AmqpMessageBuilder address(String address) {
    message.setAddress(address);
    return this;
  }

  public AmqpMessageBuilder replyTo(String replyTo) {
    message.setReplyTo(replyTo);
    return this;
  }

  public AmqpMessageBuilder correlationId(String correlationId) {
    message.setCorrelationId(correlationId);
    return this;
  }

  public AmqpMessageBuilder body(String value) {
    message.setBody(new AmqpValue(value));
    return this;
  }

  public AmqpMessageBuilder body(JsonObject value) {
    message.setBody(new AmqpValue(value.encode()));
    return this;
  }

  public AmqpMessageBuilder body(JsonArray value) {
    message.setBody(new AmqpSequence(value.getList()));
    return this;
  }

  public AmqpMessageBuilder subject(String subject) {
    message.setSubject(subject);
    return this;
  }

  public AmqpMessageBuilder contentType(String ct) {
    message.setContentType(ct);
    return this;
  }

  public AmqpMessageBuilder contentEncoding(String ct) {
    message.setContentEncoding(ct);
    return this;
  }

  public AmqpMessageBuilder expiryTime(long expiry) {
    message.setExpiryTime(expiry);
    return this;
  }

  public AmqpMessageBuilder creationTime(long ct) {
    message.setCreationTime(ct);
    return this;
  }

  public AmqpMessageBuilder ttl(long ttl) {
    message.setTtl(ttl);
    return this;
  }

  public AmqpMessageBuilder groupId(String gi) {
    message.setGroupId(gi);
    return this;
  }

  public AmqpMessageBuilder replyToGroupId(String rt) {
    message.setReplyToGroupId(rt);
    return this;
  }

  public AmqpMessageBuilder applicationProperties(JsonObject props) {
    ApplicationProperties properties = new ApplicationProperties(props.getMap());
    message.setApplicationProperties(properties);
    return this;
  }

}
