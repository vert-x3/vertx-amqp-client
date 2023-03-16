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
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.proton.ProtonConnection;

import static io.vertx.codegen.annotations.GenIgnore.PERMITTED_TYPE;

/**
 * Once connected to the broker or router, you get a connection. This connection is automatically opened.
 */
@VertxGen
public interface AmqpConnection {

  /**
   * Registers a handler called on disconnection.
   *
   * @param handler the exception handler.
   * @return the connection
   */
  @Fluent
  AmqpConnection exceptionHandler(Handler<Throwable> handler);

  /**
   * Closes the AMQP connection, i.e. allows the Close frame to be emitted.
   *
   * @return a future notified when the connection is closed
   * @return the connection
   */
  Future<Void> close();

  /**
   * Creates a receiver used to consume messages from the given address. The receiver has no handler and won't
   * start receiving messages until a handler is explicitly configured.
   *
   * @param address           The source address to attach the consumer to, must not be {@code null}
   * @return a future notified with the receiver. The receiver has been opened.
   */
  Future<AmqpReceiver> createReceiver(String address);

  /**
   * Creates a receiver used to consumer messages from the given address.
   *
   * @param address           The source address to attach the consumer to.
   * @param receiverOptions   The options for this receiver.
   * @return a future notified with the receiver, once opened. Note that the {@code messageHandler}
   *                          can be called before the {@code completionHandler} if messages are awaiting delivery.
   */
  Future<AmqpReceiver> createReceiver(String address, AmqpReceiverOptions receiverOptions);

  /**
   * Creates a dynamic receiver. The address is provided by the broker and is available in the {@code completionHandler},
   * using the {@link AmqpReceiver#address()} method. this method is useful for request-reply to generate a unique
   * reply address.
   *
   * @return a future notified when the receiver has been created and opened.
   */
  Future<AmqpReceiver> createDynamicReceiver();

  /**
   * Creates a sender used to send messages to the given address. The address must be set. For anonymous sender, check
   * {@link #createAnonymousSender()}.
   *
   * @param address           The target address to attach to, must not be {@code null}
   * @return a future notified with the sender, once opened
   * @see #createAnonymousSender()
   */
  Future<AmqpSender> createSender(String address);

  /**
   * Creates a sender used to send messages to the given address. The address must be set. For anonymous sender, check
   * {@link #createAnonymousSender()}.
   *
   * @param address           The target address to attach to, allowed to be {@code null} if the {@code options}
   *                          configures the sender to be attached to a dynamic address (provided by the broker).
   * @param options           The AMQP sender options
   * @return a future notified with the sender, once opened
   * @see #createAnonymousSender()
   */
  Future<AmqpSender> createSender(String address, AmqpSenderOptions options);

  /**
   * Creates an anonymous sender.
   * <p>
   * Unlike "regular" sender, this sender is not associated to a specific address, and each message sent must provide
   * an address. This method can be used in request-reply scenarios where you create a sender to send the reply,
   * but you don't know the address, as the reply address is passed into the message you are going to receive.
   *
   * @return a future notifid with the created sender, once opened
   */
  Future<AmqpSender> createAnonymousSender();

  /**
   * @return whether the connection has been disconnected.
   */
  boolean isDisconnected();

  /**
   * @return a future completed when the connection is closed
   */
  Future<Void> closeFuture();

  /**
   * @return the underlying ProtonConnection.
   */
  @GenIgnore(PERMITTED_TYPE)
  ProtonConnection unwrap();

}
