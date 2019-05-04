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

import io.vertx.codegen.annotations.CacheReturn;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;

/**
 * Interface used to consume AMQP message as a stream of message.
 * Back pressure is implemented using AMQP credits.
 */
@VertxGen
public interface AmqpReceiver extends ReadStream<AmqpMessage> {

  @Override
  AmqpReceiver exceptionHandler(Handler<Throwable> handler);

  @Override
  AmqpReceiver handler(@Nullable Handler<AmqpMessage> handler);

  @Override
  AmqpReceiver pause();

  @Override
  AmqpReceiver resume();

  @Override
  AmqpReceiver fetch(long amount);

  @Override
  AmqpReceiver endHandler(@Nullable Handler<Void> endHandler);

  /**
   * The listened address.
   *
   * @return the address, not {@code null}
   */
  @CacheReturn
  String address();

  /**
   * Closes the receiver.
   *
   * @param handler handler called when the receiver has been closed, can be {@code null}
   */
  void close(Handler<AsyncResult<Void>> handler);

  /**
   * Gets the connection having created the receiver. Cannot be {@code null}
   *
   * @return the connection having created the receiver.
   */
  AmqpConnection connection();
}
