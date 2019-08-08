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

import io.vertx.amqp.impl.AmqpClientImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.Objects;

/**
 * AMQP Client entry point.
 * Use this interface to create an instance of {@link AmqpClient} and connect to a broker and server.
 */
@VertxGen
public interface AmqpClient {

  /**
   * Creates a new instance of {@link AmqpClient} using an internal Vert.x instance (with default configuration) and
   * the given AMQP client configuration. Note that the created Vert.x instance will be closed when the client is
   * closed.
   *
   * @param options the AMQP client options, may be {@code null} falling back to the default configuration
   * @return the created instances.
   */
  static AmqpClient create(AmqpClientOptions options) {
    return new AmqpClientImpl(Vertx.vertx(), options, true);
  }

  /**
   * Creates a new instance of {@link AmqpClient} with the given Vert.x instance and the given options.
   *
   * @param vertx   the vert.x instance, must not be {@code null}
   * @param options the AMQP options, may be @{code null} falling back to the default configuration
   * @return the AMQP client instance
   */
  static AmqpClient create(Vertx vertx, AmqpClientOptions options) {
    return new AmqpClientImpl(Objects.requireNonNull(vertx), options, false);
  }

  /**
   * Connects to the AMQP broker or router. The location is specified in the {@link AmqpClientOptions} as well as the
   * potential credential required.
   *
   * @param connectionHandler handler that will process the result, giving either the connection or failure cause. Must
   *                          not be {@code null}.
   */
  @Fluent
  AmqpClient connect(Handler<AsyncResult<AmqpConnection>> connectionHandler);

  Future<AmqpConnection> connect();

  /**
   * Closes the client.
   * The client must always be closed once not needed anymore.
   *
   * @param closeHandler the close handler notified when the operation completes. It can be {@code null}.
   */
  void close(@Nullable Handler<AsyncResult<Void>> closeHandler);

  Future<Void> close();

  /**
   * Creates a receiver used to consume messages from the given address. The receiver has no handler and won't
   * start receiving messages until a handler is explicitly configured. This method avoids having to connect explicitly.
   * You can retrieve the connection using {@link AmqpReceiver#connection()}.
   *
   * @param address           The source address to attach the consumer to, must not be {@code null}
   * @param completionHandler the handler called with the receiver. The receiver has been opened.
   * @return the client.
   */
  @Fluent
  AmqpClient createReceiver(String address, Handler<AsyncResult<AmqpReceiver>> completionHandler);

  Future<AmqpReceiver> createReceiver(String address);

  /**
   * Creates a receiver used to consumer messages from the given address.  This method avoids having to connect
   * explicitly. You can retrieve the connection using {@link AmqpReceiver#connection()}.
   *
   * @param address           The source address to attach the consumer to.
   * @param receiverOptions   The options for this receiver.
   * @param completionHandler The handler called with the receiver, once opened. Note that the {@code messageHandler}
   *                          can be called before the {@code completionHandler} if messages are awaiting delivery.
   * @return the connection.
   */
  @Fluent
  AmqpClient createReceiver(String address, AmqpReceiverOptions receiverOptions,
    Handler<AsyncResult<AmqpReceiver>> completionHandler);

  Future<AmqpReceiver> createReceiver(String address, AmqpReceiverOptions receiverOptions);

  /**
   * Creates a sender used to send messages to the given address. The address must be set.
   *
   * @param address           The target address to attach to, must not be {@code null}
   * @param completionHandler The handler called with the sender, once opened
   * @return the client.
   */
  @Fluent
  AmqpClient createSender(String address, Handler<AsyncResult<AmqpSender>> completionHandler);

  Future<AmqpSender> createSender(String address);

  /**
   * Creates a sender used to send messages to the given address. The address must be set.
   *
   * @param address           The target address to attach to, must not be {@code null}
   * @param options The options for this sender.
   * @param completionHandler The handler called with the sender, once opened
   * @return the client.
   */
  @Fluent
  AmqpClient createSender(String address, AmqpSenderOptions options,
                          Handler<AsyncResult<AmqpSender>> completionHandler);

  Future<AmqpSender> createSender(String address, AmqpSenderOptions options);

}
