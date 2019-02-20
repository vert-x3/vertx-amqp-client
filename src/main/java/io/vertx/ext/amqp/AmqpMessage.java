package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.amqp.impl.AmqpMessageBuilderImpl;
import org.apache.qpid.proton.message.Message;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a AMQP message.
 */
@VertxGen
public interface AmqpMessage {


  static AmqpMessageBuilder create() {
    return new AmqpMessageBuilderImpl();
  }

  static AmqpMessageBuilder create(AmqpMessage existing) {
    return new AmqpMessageBuilderImpl(existing);
  }

  @GenIgnore
  static AmqpMessageBuilder create(Message existing) {
    return new AmqpMessageBuilderImpl(existing);
  }

  boolean isDurable();

  boolean isFirstAcquirer();

  int priority();

  String id();

  String address();

  String replyTo();

  String correlationId();

  boolean isBodyNull();

  boolean bodyAsBoolean();

  byte bodyAsByte();

  short bodyAsShort();

  int bodyAsInteger();

  long bodyAsLong();

  float bodyAsFloat();

  double bodyAsDouble();

  char bodyAsChar();

  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  Instant bodyAsTimestamp();

  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  UUID bodyAsUUID();

  Buffer bodyAsBinary();

  String bodyAsString();

  String bodyAsSymbol();

  <T> List<T> bodyAsList();

  @GenIgnore
  <K, V> Map<K, V> bodyAsMap();

  JsonObject bodyAsJsonObject();

  JsonArray bodyAsJsonArray();

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

  JsonObject getApplicationProperties();

  /**
   * Allows replying to an incoming message.
   * This method is only available is: 1) reply is enabled, 2) the message has been received. Otherwise a
   * {@link IllegalStateException} is thrown.
   *
   * @param message the message
   * @return the current message.
   */
  @Fluent
  AmqpMessage reply(AmqpMessage message);

  /**
   * Allows replying to an incoming message and expecting another reply.
   * This method is only available is: 1) reply is enabled, 2) the message has been received. Otherwise a
   * {@link IllegalStateException} is thrown.
   *
   * @param message the message
   * @param replyToReplyHandler a handler receiving the reply to this reply, must not be {@code null}
   * @return the current message.
   */
  @Fluent
  AmqpMessage reply(AmqpMessage message, Handler<AsyncResult<AmqpMessage>> replyToReplyHandler);

  //TODO What type should we use for delivery annotations and message annotations

  // TODO Add header/ footer


}
