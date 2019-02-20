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
 * Represents an AMQP message.
 *
 * Reference about the different metadata can be found on
 * <a href="http://docs.oasis-open.org/amqp/core/v1.0/amqp-core-messaging-v1.0.html#type-properties">AMQP message properties</a>.
 *
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

  /**
   * @return the message properties as JSON object.
   */
  JsonObject applicationProperties();

  @GenIgnore
  Message unwrap();

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
   * @param message             the message
   * @param replyToReplyHandler a handler receiving the reply to this reply, must not be {@code null}
   * @return the current message.
   */
  @Fluent
  AmqpMessage reply(AmqpMessage message, Handler<AsyncResult<AmqpMessage>> replyToReplyHandler);

  //TODO What type should we use for delivery annotations and message annotations

  // TODO Add header/ footer


}
