package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

@VertxGen
public interface AmqpSender extends WriteStream<AmqpMessage> {

  /**
   * Sends an AMQP message. The destination the configured sender address or the address configured in the message.
   *
   * @param message the message, must not be {@code null}
   * @return the current sender
   */
  @Fluent
  AmqpSender send(AmqpMessage message);


  /**
   * Sends an AMQP message and expect a reply.Sends an AMQP message. The destination the configured sender address or
   * the address configured in the message.
   * If the reply does not arrive before the timeout configured in the AMQP client options, the reply handler
   * is called with a failure. Otherwise the reply handler is called with the response message.
   *
   * @param message the message, must not be {@code null}
   * @param reply   the reply handler, must not be {@code null}
   * @return the current sender.
   */
  @Fluent
  AmqpSender send(AmqpMessage message, Handler<AsyncResult<AmqpMessage>> reply);

  /**
   * Sends an AMQP message to the given address. This address overrides the address specified in the message if any.
   *
   * @param address the address, must not be {@code null}
   * @param message the message, must not be {@code null}
   * @return the current sender
   */
  @Fluent
  AmqpSender send(String address, AmqpMessage message);

  /**
   * Sends an AMQP message to the given address and expect a reply. This address overrides the address specified in the
   * message if any. If the reply does not arrive before the timeout configured in the AMQP client options, the reply
   * handler is called with a failure. Otherwise the reply handler is called with the response message.
   *
   * @param address the address, must not be {@code null}
   * @param message the message, must not be {@code null}
   * @param reply   the reply handler, must not be {@code null}
   * @return the current sender.
   */
  @Fluent
  AmqpSender send(String address, AmqpMessage message, Handler<AsyncResult<AmqpMessage>> reply);

  /**
   * Sends an AMQP message and waits for an acknowledgement. The acknowledgement handler is called with an
   * {@link AsyncResult} marked as failed if the message has been rejected or re-routed. If the message has been accepted,
   * the handler is called with a success.
   *
   * @param message                the message, must not be {@code null}
   * @param acknowledgementHandler the acknowledgement handler, must not be {@code null}
   * @return the current sender
   */
  @Fluent
  AmqpSender sendWithAck(AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler);

  /**
   * Sends an AMQP message and waits for an acknowledgement. The acknowledgement handler is called with an
   * {@link AsyncResult} marked as failed if the message has been rejected or re-routed. If the message has been accepted,
   * the handler is called with a success.
   * <p>
   * The message is sent to the given address.
   *
   * @param address                the address, must not be {@code null}
   * @param message                the message, must not be {@code null}
   * @param acknowledgementHandler the acknowledgement handler, must not be {@code null}
   * @return the current sender
   */
  @Fluent
  AmqpSender sendWithAck(String address, AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler);

  /**
   * Closes the sender.
   *
   * @param handler called when the sender has been closed, must not be {@code null}
   */
  void close(Handler<AsyncResult<Void>> handler);

  /**
   * @return the configured address.
   */
  String address();
}
