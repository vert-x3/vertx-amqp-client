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
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.future.PromiseInternal;
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
    Objects.requireNonNull(connectionHandler, "Handler must not be null");
    connect().onComplete(connectionHandler);
    return this;
  }

  @Override
  public Future<AmqpConnection> connect() {
    Objects.requireNonNull(options.getHost(), "Host must be set");
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    PromiseInternal<AmqpConnection> promise = ctx.promise();
    new AmqpConnectionImpl(ctx, options, proton, promise);
    Future<AmqpConnection> future = promise.future();
    future.onSuccess(conn -> {
      connections.add(conn);
      conn.closeFuture().onComplete(ar -> {
        connections.remove(conn);
      });
    });
    return future;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    List<Future> actions = new ArrayList<>();
    for (AmqpConnection connection : connections) {
      actions.add(connection.close());
    }

    CompositeFuture.join(actions).onComplete(done -> {
      connections.clear();
      if (mustCloseVertxOnClose) {
        vertx
          .close()
          .onComplete(x -> {
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
  public Future<Void> close() {
    Promise<Void> promise = Promise.promise();
    close(promise);
    return promise.future();
  }

  @Override
  public AmqpClient createReceiver(String address,
    Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    return connect(res -> {
      if (res.failed()) {
        completionHandler.handle(res.mapEmpty());
      } else {
        res.result().createReceiver(address).onComplete(completionHandler);
      }
    });
  }

  @Override
  public Future<AmqpReceiver> createReceiver(String address) {
    Promise<AmqpReceiver> promise = Promise.promise();
    createReceiver(address, promise);
    return promise.future();
  }

  @Override
  public AmqpClient createReceiver(String address, AmqpReceiverOptions receiverOptions, Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    return connect(res -> {
      if (res.failed()) {
        completionHandler.handle(res.mapEmpty());
      } else {
        res.result().createReceiver(address, receiverOptions).onComplete(completionHandler);
      }
    });
  }

  @Override
  public Future<AmqpReceiver> createReceiver(String address, AmqpReceiverOptions receiverOptions) {
    Promise<AmqpReceiver> promise = Promise.promise();
    createReceiver(address, receiverOptions, promise);
    return promise.future();
  }

  @Override
  public AmqpClient createSender(String address, Handler<AsyncResult<AmqpSender>> completionHandler) {
    return connect(res -> {
      if (res.failed()) {
        completionHandler.handle(res.mapEmpty());
      } else {
        res.result().createSender(address).onComplete(completionHandler);
      }
    });
  }

  @Override
  public Future<AmqpSender> createSender(String address) {
    Promise<AmqpSender> promise = Promise.promise();
    createSender(address, promise);
    return promise.future();
  }

  @Override
  public AmqpClient createSender(String address, AmqpSenderOptions options,
                                 Handler<AsyncResult<AmqpSender>> completionHandler) {
    return connect(res -> {
      if (res.failed()) {
        completionHandler.handle(res.mapEmpty());
      } else {
        res.result().createSender(address, options).onComplete(completionHandler);
      }
    });
  }

  @Override
  public Future<AmqpSender> createSender(String address, AmqpSenderOptions options) {
    Promise<AmqpSender> promise = Promise.promise();
    createSender(address, options, promise);
    return promise.future();
  }

  public int numConnections() {
    return connections.size();
  }
}
