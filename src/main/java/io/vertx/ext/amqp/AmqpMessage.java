package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

@VertxGen
public interface AmqpMessage {

  boolean isDurable();

  @Fluent
  AmqpMessage setDurable(boolean durable);

  boolean isFirstAcquirer();

  @Fluent
  AmqpMessage setFirstAcquirer(boolean fa);

  int priority();

  @Fluent
  AmqpMessage setPriority(int p);

  String id();

  @Fluent
  AmqpMessage setId(String id);

  String address();

  @Fluent
  AmqpMessage setAddress(String address);

  String replyTo();

  @Fluent
  AmqpMessage setReplyTo(String rep);

  String correlationId();

  @Fluent
  AmqpMessage setCorrelationId(String correlationId);

  Buffer body();

  <T> T bodyAs(Class<T> target);

  @Fluent
  AmqpMessage setBody(Buffer buffer);

  String subject();

  @Fluent
  AmqpMessage setSubject(String subject);

  String contentType();

  @Fluent
  AmqpMessage setContentType(String ct);

  String contentEncoding();

  @Fluent
  AmqpMessage setContentEncoding(String ce);

  long expiryTime();

  @Fluent
  AmqpMessage setExpiryTime(long ttl);

  long creationTime();

  @Fluent
  AmqpMessage setCreationTime(long now);

  long ttl();

  @Fluent
  AmqpMessage setTtl(long ttl);

  int deliveryCount();

  @Fluent
  AmqpMessage setDeliveryCount(int count);

  String groupId();

  @Fluent
  AmqpMessage setGroupId(String groupId);

  String replyToGroupId();

  @Fluent
  AmqpMessage setReplyToGroupId(String groupId);

  int groupSequence();

  @Fluent
  AmqpMessage setGroupSequence(int seq);

  JsonObject applicationProperties();

  @Fluent
  AmqpMessage setApplicationProperties(JsonObject json);

  //TODO What type should we use for delivery annotations and message annotations

  // TODO Add header/ footer


}
