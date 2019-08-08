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
package io.vertx.amqp;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Once connected to the broker or router, you get a connection. This connection is automatically opened.
 */
@VertxGen
public interface AmqpConnection {

  /**
   * Registers a handler called on disconnection.
   *
   * @param handler the exception handler.
   */
  @Fluent
  AmqpConnection exceptionHandler(Handler<Throwable> handler);

  /**
   * Closes the AMQP connection, i.e. allows the Close frame to be emitted.
   *
   * @param done the close handler notified when the connection is closed. May be {@code null}.
   * @return the connection
   */
  @Fluent
  AmqpConnection close(Handler<AsyncResult<Void>> done);

  Future<Void> close();

  /**
   * Creates a receiver used to consume messages from the given address. The receiver has no handler and won't
   * start receiving messages until a handler is explicitly configured.
   *
   * @param address           The source address to attach the consumer to, must not be {@code null}
   * @param completionHandler the handler called with the receiver. The receiver has been opened.
   * @return the connection.
   */
  @Fluent
  AmqpConnection createReceiver(String address, Handler<AsyncResult<AmqpReceiver>> completionHandler);

  Future<AmqpReceiver> createReceiver(String address);

  /**
   * Creates a receiver used to consumer messages from the given address.
   *
   * @param address           The source address to attach the consumer to.
   * @param receiverOptions   The options for this receiver.
   * @param completionHandler The handler called with the receiver, once opened. Note that the {@code messageHandler}
   *                          can be called before the {@code completionHandler} if messages are awaiting delivery.
   * @return the connection.
   */
  @Fluent
  AmqpConnection createReceiver(String address, AmqpReceiverOptions receiverOptions,
    Handler<AsyncResult<AmqpReceiver>> completionHandler);

  Future<AmqpReceiver> createReceiver(String address, AmqpReceiverOptions receiverOptions);

  /**
   * Creates a dynamic receiver. The address is provided by the broker and is available in the {@code completionHandler},
   * using the {@link AmqpReceiver#address()} method. this method is useful for request-reply to generate a unique
   * reply address.
   *
   * @param completionHandler the completion handler, called when the receiver has been created and opened.
   * @return the connection.
   */
  @Fluent
  AmqpConnection createDynamicReceiver(Handler<AsyncResult<AmqpReceiver>> completionHandler);

  Future<AmqpReceiver> createDynamicReceiver();

  /**
   * Creates a sender used to send messages to the given address. The address must be set. For anonymous sender, check
   * {@link #createAnonymousSender(Handler)}.
   *
   * @param address           The target address to attach to, must not be {@code null}
   * @param completionHandler The handler called with the sender, once opened
   * @return the connection.
   * @see #createAnonymousSender(Handler)
   */
  @Fluent
  AmqpConnection createSender(String address, Handler<AsyncResult<AmqpSender>> completionHandler);

  Future<AmqpSender> createSender(String address);

  /**
   * Creates a sender used to send messages to the given address. The address must be set. For anonymous sender, check
   * {@link #createAnonymousSender(Handler)}.
   *
   * @param address           The target address to attach to, allowed to be {@code null} if the {@code options}
   *                          configures the sender to be attached to a dynamic address (provided by the broker).
   * @param options           The AMQP sender options
   * @param completionHandler The handler called with the sender, once opened
   * @return the connection.
   * @see #createAnonymousSender(Handler)
   */
  @Fluent
  AmqpConnection createSender(String address, AmqpSenderOptions options,
    Handler<AsyncResult<AmqpSender>> completionHandler);

  Future<AmqpSender> createSender(String address, AmqpSenderOptions options);

  /**
   * Creates an anonymous sender.
   * <p>
   * Unlike "regular" sender, this sender is not associated to a specific address, and each message sent must provide
   * an address. This method can be used in request-reply scenarios where you create a sender to send the reply,
   * but you don't know the address, as the reply address is passed into the message you are going to receive.
   *
   * @param completionHandler The handler called with the created sender, once opened
   * @return the connection.
   */
  @Fluent
  AmqpConnection createAnonymousSender(Handler<AsyncResult<AmqpSender>> completionHandler);

  Future<AmqpSender> createAnonymousSender();

}
