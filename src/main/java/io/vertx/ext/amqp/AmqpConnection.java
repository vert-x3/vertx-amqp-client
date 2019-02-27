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
package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;


/**
 * Once connected to the broker or router, you get a connection. This connection is automatically opened.
 */
@VertxGen
public interface AmqpConnection {

  /**
   * Registers a handler called on disconnection.
   *
   * @param endHandler the end handler.
   */
  @Fluent
  AmqpConnection endHandler(Handler<Void> endHandler);

  /**
   * Closes the AMQP connection, i.e. allows the Close frame to be emitted.
   *
   * @param done the close handler notified when the connection is closed. May be {@code null}.
   * @return the connection
   */
  @Fluent
  AmqpConnection close(Handler<AsyncResult<Void>> done);

  /**
   * Creates a receiver used to consume messages from the given address. The receiver has no handler and won't
   * start receiving messages until a handler is explicitly configured.
   *
   * @param address           The source address to attach the consumer to, must not be {@code null}
   * @param completionHandler the handler called with the receiver. The receiver has been opened.
   * @return the connection.
   */
  @Fluent
  AmqpConnection receiver(String address, Handler<AsyncResult<AmqpReceiver>> completionHandler);

  /**
   * Creates a receiver used to consume messages from the given address.
   *
   * @param address           The source address to attach the consumer to, must not be {@code null}
   * @param messageHandler    The message handler, must not be {@code null}
   * @param completionHandler the handler called with the receiver that has been opened. Note that the
   *                          {@code messageHandler} can be called before the {@code completionHandler} if messages
   *                          are awaiting delivery.
   * @return the connection.
   */
  @Fluent
  AmqpConnection receiver(String address, Handler<AmqpMessage> messageHandler, Handler<AsyncResult<AmqpReceiver>> completionHandler);

  /**
   * Creates a receiver used to consumer messages from the given address.
   *
   * @param address           The source address to attach the consumer to.
   * @param receiverOptions   The options for this receiver.
   * @param messageHandler    The message handler, must not be {@code null}
   * @param completionHandler The handler called with the receiver, once opened. Note that the {@code messageHandler}
   *                          can be called before the {@code completionHandler} if messages are awaiting delivery.
   * @return the connection.
   */
  @Fluent
  AmqpConnection receiver(String address, AmqpReceiverOptions receiverOptions, Handler<AmqpMessage> messageHandler,
                          Handler<AsyncResult<AmqpReceiver>> completionHandler);

  /**
   * Creates a sender used to send messages to the given address. If no address (i.e null) is specified then a
   * sender will be established to the 'anonymous relay' and each message must specify its destination address.
   *
   * @param address           The target address to attach to, or {@code null} to attach to the anonymous relay.
   * @param completionHandler The handler called with the sender, once opened
   * @return the connection.
   */
  @Fluent
  AmqpConnection sender(String address, Handler<AsyncResult<AmqpSender>> completionHandler);

  /**
   * Sets a handler for when an AMQP {@code Close} frame is received from the remote peer.
   *
   * @param remoteCloseHandler the handler
   * @return the connection
   */
  @Fluent
  AmqpConnection closeHandler(Handler<AmqpConnection> remoteCloseHandler);

}
