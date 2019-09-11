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
package io.vertx.amqp.impl;

import io.vertx.amqp.*;
import io.vertx.core.*;
import io.vertx.proton.ProtonClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class AmqpClientImpl implements AmqpClient {

  private final Vertx vertx;
  private final ProtonClient proton;
  private final AmqpClientOptions options;

  private final List<AmqpConnection> connections = new CopyOnWriteArrayList<>();
  private final boolean mustCloseVertxOnClose;

  public AmqpClientImpl(Vertx vertx, AmqpClientOptions options, boolean mustCloseVertxOnClose) {
    this.vertx = vertx;
    if (options == null) {
      this.options = new AmqpClientOptions();
    } else {
      this.options = options;
    }
    this.proton = ProtonClient.create(vertx);
    this.mustCloseVertxOnClose = mustCloseVertxOnClose;
  }

  @Override
  public AmqpClient connect(Handler<AsyncResult<AmqpConnection>> connectionHandler) {
    Objects.requireNonNull(options.getHost(), "Host must be set");
    Objects.requireNonNull(connectionHandler, "Handler must not be null");
    new AmqpConnectionImpl(vertx.getOrCreateContext(), this, options, proton, connectionHandler);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    List<Future> actions = new ArrayList<>();
    for (AmqpConnection connection : connections) {
      Future<Void> future = Future.future();
      connection.close(future);
      actions.add(future);
    }

    CompositeFuture.join(actions).setHandler(done -> {
      connections.clear();
      if (mustCloseVertxOnClose) {
        vertx.close(x -> {
          if (done.succeeded() && x.succeeded()) {
            if (handler != null) {
              handler.handle(Future.succeededFuture());
            }
          } else {
            if (handler != null) {
              handler.handle(Future.failedFuture(done.failed() ? done.cause() : x.cause()));
            }
          }
        });
      } else if (handler != null) {
        handler.handle(done.mapEmpty());
      }
    });
  }

  @Override
  public AmqpClient createReceiver(String address,
    Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    return connect(res -> {
      if (res.failed()) {
        completionHandler.handle(res.mapEmpty());
      } else {
        res.result().createReceiver(address, completionHandler);
      }
    });
  }

  @Override
  public AmqpClient createReceiver(String address, AmqpReceiverOptions receiverOptions, Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    return connect(res -> {
      if (res.failed()) {
        completionHandler.handle(res.mapEmpty());
      } else {
        res.result().createReceiver(address, receiverOptions, completionHandler);
      }
    });
  }

  @Override
  public AmqpClient createSender(String address, Handler<AsyncResult<AmqpSender>> completionHandler) {
    return connect(res -> {
      if (res.failed()) {
        completionHandler.handle(res.mapEmpty());
      } else {
        res.result().createSender(address, completionHandler);
      }
    });
  }

  @Override
  public AmqpClient createSender(String address, AmqpSenderOptions options,
                                 Handler<AsyncResult<AmqpSender>> completionHandler) {
    return connect(res -> {
      if (res.failed()) {
        completionHandler.handle(res.mapEmpty());
      } else {
        res.result().createSender(address, options, completionHandler);
      }
    });
  }

  synchronized void register(AmqpConnectionImpl connection) {
    connections.add(connection);
  }
}
