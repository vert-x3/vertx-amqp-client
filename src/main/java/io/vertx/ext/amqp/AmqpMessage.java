package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.apache.qpid.proton.message.Message;

@VertxGen
public interface AmqpMessage {

  @GenIgnore
  static AmqpMessageBuilder create() {
    return new AmqpMessageBuilder();
  }

  @GenIgnore
  static AmqpMessageBuilder create(AmqpMessage existing) {
    return new AmqpMessageBuilder(existing);
  }

  @GenIgnore
  static AmqpMessageBuilder create(Message existing) {
    return new AmqpMessageBuilder(existing);
  }

  boolean isDurable();

  boolean isFirstAcquirer();

  int priority();

  String id();

  String address();

  String replyTo();

  String correlationId();

  Buffer body();

  <T> T bodyAs(Class<T> target);

  String subject();

  String contentType();

  String contentEncoding();

  long expiryTime();

  long creationTime();

  long ttl();

  long deliveryCount();

  String groupId();

  String replyToGroupId();

  long groupSequence();

  JsonObject applicationProperties();

  @GenIgnore
  Message unwrap();

  //TODO What type should we use for delivery annotations and message annotations

  // TODO Add header/ footer


}
