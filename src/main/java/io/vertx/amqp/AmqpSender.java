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
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

/**
 * AMQP Sender interface used to send messages.
 */
@VertxGen
public interface AmqpSender extends WriteStream<AmqpMessage> {

  @Override
  AmqpSender write(AmqpMessage data);

  @Override
  AmqpSender write(AmqpMessage data, Handler<AsyncResult<Void>> handler);

  @Override
  AmqpSender exceptionHandler(Handler<Throwable> handler);

  @Override
  AmqpSender setWriteQueueMaxSize(int maxSize);

  /**
   * Sends an AMQP message. The destination the configured sender address or the address configured in the message.
   *
   * @param message the message, must not be {@code null}
   * @return the current sender
   */
  @Fluent
  AmqpSender send(AmqpMessage message);

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
   * Closes the sender.
   *
   * @param handler called when the sender has been closed, must not be {@code null}
   */
  void close(Handler<AsyncResult<Void>> handler);

  /**
   * @return the configured address.
   */
  String address();

  /**
   * Gets the connection having created the sender. Cannot be {@code null}
   *
   * @return the connection having created the sender.
   */
  AmqpConnection connection();

  /**
   * @return the remaining credit, 0 is none.
   */
  long remainingCredits();
}
